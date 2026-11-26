package de.smartdata.jobs.threads;

import de.smartdata.jobs.actions.*;
import de.smartdata.jobs.data.SmartDataAccessor;
import de.smartdata.jobs.data.SmartDataAccessorException;
import de.smartdata.jobs.data.RunRecord;
import de.fhbielefeld.scl.rest.util.JSONFormatter;
import de.ngi.logging.Logger;
import de.ngi.restutils.DateTimeParser;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages and runs jobs.
 *
 * Every execution creates a {@link RunRecord} that carries the run identity,
 * lineage and input snapshot written to datajobs_log alongside the classic
 * result columns (replay-fields.md §3.2):
 *
 * - runid: uuid minted by this runner (never by the database), so the id is
 *   known before the log row exists and child runs can reference it.
 * - chainrunid: the logical chain traversal. A root execution (schedule tick
 *   or manual start) opens its own chain run: chainrunid = runid. Successor
 *   jobs inherit the chainrunid of their parent run.
 * - parentrunid: the run that triggered this one (null for root runs).
 * - step: position in the chain run (root = 0, successors = parent step + 1).
 *
 * The derived booleans iserror/wastimeout keep their meaning; runstate holds
 * the normalised state and errorclass the coarse failure taxonomy.
 *
 * @author Florian Fehring
 */
public class JobRunner {

    // CHANGED from ManagedScheduledExecutorService to ScheduledExecutorService
    private ScheduledExecutorService executorService;
    private ScheduledExecutorService priorityExecutor;

    private Map<String, Future<?>> futures = new java.util.concurrent.ConcurrentHashMap<>();
    private final Set<String> executingJobs = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Set<String> stoppedWhileExecuting = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private boolean stopped = false;

    // Singleton instance
    private static volatile JobRunner instance;

    // Private constructor
    private JobRunner() {
        // Empty - initialization happens in JobRunnerInitializer
    }

    // getInstance method
    public static JobRunner getInstance() {
        if (instance == null) {
            synchronized (JobRunner.class) {
                if (instance == null) {
                    instance = new JobRunner();
                }
            }
        }
        return instance;
    }

    /**
     * Set the executor service (called by JobRunnerInitializer)
     *
     * @param executorService the executor service to use
     */
    public void setExecutorService(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Set the priority executor (called by JobRunnerInitializer)
     *
     * @param priorityExecutor the priority executor to use
     */
    public void setPriorityExecutor(ScheduledExecutorService priorityExecutor) {
        this.priorityExecutor = priorityExecutor;
    }

    /**
     * Start all defined jobs
     *
     * @param smartdataurl URL to the SmartData Instance that contains job
     * informations
     * @param collection Name of the collection contianing job informations
     * @param storage Storage containing job informations
     * @return JsonArray with information for every job
     * @throws JobRunnerException
     */
    public JsonArray startAll(String smartdataurl, String collection, String storage) throws JobRunnerException {
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        this.stopped = false;
        try {
            // Jobs array from database
            JsonObjectBuilder result = Json.createObjectBuilder();
            JsonArray jobs = acc.fetchData(storage, collection, null, null, null, null, null, null, null, result);
            // Jobs array for output (because fetched one is not modifyable)
            JsonArrayBuilder jobinfos = Json.createArrayBuilder();
            for (JsonObject job : jobs.getValuesAs(JsonObject.class)) {
                // Do not start jobs without start time
                Boolean active = job.getBoolean("active", false);
                if (active == false) {
                    continue;
                }

                JsonObject jobinfo = this.executeJob(smartdataurl, collection, storage, job, acc, null);
                if (jobinfo.containsKey("result")) {
                    this.startJobChain(smartdataurl, collection, storage, jobinfo);
                }
                jobinfos.add(jobinfo);
            }
            return jobinfos.build();
        } catch (SmartDataAccessorException ex) {
            JobRunnerException jrex = new JobRunnerException(ex.getLocalizedMessage());
            jrex.initCause(ex);
            throw jrex;
        }
    }

    /**
     * Starts one single job. Used for manually started jobs and for
     * successor jobs of a chain.
     *
     * @param smartdataurl URL to the SmartData Instance that contains job
     * informations
     * @param collection Name of the collection contianing job informations
     * @param storage Storage containing job informations
     * @param jobid Jobs id
     * @param preJobInfo Result of the previous job (if started over a chain)
     * @return Job execution result
     * @throws JobRunnerException
     */
    public JsonArray start(String smartdataurl, String collection, String storage, Long jobid, JsonObject preJobInfo) throws JobRunnerException {
        return this.start(smartdataurl, collection, storage, jobid, preJobInfo, null);
    }

    /**
     * Starts one single job with explicit run bookkeeping. When the run
     * record is null (manual start / schedule tick) a new root run is opened:
     * a fresh runid that is also the chainrunid, step 0, no parent.
     */
    private JsonArray start(String smartdataurl, String collection, String storage, Long jobid, JsonObject preJobInfo, RunRecord run) throws JobRunnerException {
        Logger.log("->start()");
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        this.stopped = false;
        try {
            // Jobs array from database
            JsonObjectBuilder result = Json.createObjectBuilder();
            JsonArray jobs = acc.fetchData(storage, collection, null, "id,eq," + jobid, null, null, null, null, null, result);
            // Jobs array for output (because fetched one is not modifyable)
            JsonArrayBuilder jobinfos = Json.createArrayBuilder();
            for (JsonObject job : jobs.getValuesAs(JsonObject.class)) {
                JsonObject jobinfo = this.executeJob(smartdataurl, collection, storage, job, acc, preJobInfo, run);
                JsonObjectBuilder jobinfo2 = Json.createObjectBuilder();
                jobinfo.forEach(jobinfo2::add);
                if (jobinfo.containsKey("result")) {
                    jobinfo2.add("chainedJobs", this.startJobChain(smartdataurl, collection, storage, jobinfo));
                }
                jobinfos.add(jobinfo2.build());
            }
            return jobinfos.build();
        } catch (SmartDataAccessorException ex) {
            JobRunnerException jrex = new JobRunnerException(ex.getLocalizedMessage());
            jrex.initCause(ex);
            throw jrex;
        }
    }

    /**
     * Exeutes a single job
     *
     * @param smartdataurl URL to the SmartData Instance that contains job
     * informations
     * @param collection Name of the collection contianing job informations
     * @param storage Storage containing job informations
     * @param job Job information object
     * @param acc SmartDataAccessor
     * @param preJobInfo Result of the previous job (chains)
     * @param run Run bookkeeping; when null a new root run is opened
     * @return JsonObject with job info and execution results
     */
    private JsonObject executeJob(String smartdataurl, String collection, String storage, JsonObject job, SmartDataAccessor acc, JsonObject preJobInfo, RunRecord run) {
        Logger.log("executeJob(" + job.getInt("id") + ")");

        // Create Json for returning info about executed (onc time execution) or sheduled (recurring execution) job
        JsonObjectBuilder returnJson = Json.createObjectBuilder(job);
        Long jobid = job.getJsonNumber("id").longValue();

        // Open run record when not called from a chain
        if (run == null) {
            run = this.newRootRun();
        }
        final RunRecord runRecord = run;

        // Check if job is active
        boolean active = job.getBoolean("active", false);
        if (!active) {
            returnJson.add("state", "Job >" + job.getString("name") + "< is currently inactive.");
            return returnJson.build();
        }

        try {
            // Get jobs action
            final Action jobsaction = getJobAction(job);

            // Jump to next job if this one is misconfigured
            if (jobsaction == null) {
                returnJson.add("state", "failed");
                returnJson.add("statemsg", "There is no action with name >" + job.getString("action", "") + "< implemented.");
                runRecord.setRunstate("failed");
                runRecord.setErrorclass("misconfigured");
                JsonObject result = returnJson.build();
                saveResultLog(smartdataurl, collection, storage, jobid, result, runRecord, true, false);
                return result;
            }

            jobsaction.setId(jobid);
            jobsaction.setPreJobsInfo(preJobInfo);

            // Get job parameters
            JsonObjectBuilder result1 = Json.createObjectBuilder();
            JsonArray jobparams = acc.fetchData(storage, collection + "_params", null, "datajob_id,eq," + job.getInt("id"), null, null, null, null, null, result1);
            JsonObject jobparamobj = loadJobParams(jobparams, returnJson);

            // A misconfigured job fails alone instead of aborting the whole schedule
            if (jobparamobj == null) {
                returnJson.add("state", "failed");
                returnJson.add("statemsg", "Job is misconfigured: at least one parameter could not be loaded (see paramError).");
                runRecord.setRunstate("failed");
                runRecord.setErrorclass("misconfigured");
                JsonObject result = returnJson.build();
                saveResultLog(smartdataurl, collection, storage, jobid, result, runRecord, true, false);
                return result;
            }

            try {
                jobsaction.setParams(jobparamobj);
                returnJson.add("params", jobparamobj);
            } catch (ActionConfigurationException acex) {
                returnJson.add("state", "failed");
                returnJson.add("statemsg", "Missconfigured action >" + job.getString("action") + "< Action reported: " + acex.getLocalizedMessage());
                runRecord.setRunstate("failed");
                runRecord.setErrorclass("misconfigured");
                JsonObject result = returnJson.build();
                saveResultLog(smartdataurl, collection, storage, jobid, result, runRecord, true, false);
                return result;
            }

            // Snapshot the input (replay-fields.md G4): the effective payload
            // of this run = configured params merged with the preJobsInfo
            // handed over by the predecessor. Written to the run record so a
            // replay can restore it without re-deriving.
            JsonObject inputSnapshot = Json.createObjectBuilder()
                    .add("params", jobparamobj)
                    .add("prejobinfo", preJobInfo != null ? preJobInfo : JsonValue.NULL)
                    .build();
            runRecord.setInput(inputSnapshot.toString());
            runRecord.setInputhash(this.inputHash(inputSnapshot));
            returnJson.add("inputhash", runRecord.getInputhash());

            // Pase Job configuration
            LocalDateTime now = LocalDateTime.now();

            final LocalDateTime start = job.getString("start", null) != null
                    ? DateTimeParser.parseTimestamp(job.getString("start")).toLocalDateTime()
                    : now;

            final LocalDateTime end = job.getString("stop", null) != null
                    ? DateTimeParser.parseTimestamp(job.getString("stop")).toLocalDateTime()
                    : null;

            long repeatsecs = job.getInt("repeatsecs", 0);
            long delay = 0;
            if (start != null) {
                delay = this.calculateDelay(start, repeatsecs);
            }

            if (delay < 0) {
                returnJson.add("state", "Job not started because start time lies in the past.");
                return returnJson.build();
            }

            returnJson.add("firstexecution", LocalDateTime.now().plusSeconds(delay).toString());

            int priority = job.getInt("priority", 0);

            Future<?> future;
            Runnable runnableTask = () -> runJobAsync(jobsaction, smartdataurl, collection, storage, job, jobid, end, jobparamobj, runRecord);
            if (repeatsecs == 0) {
                returnJson.add("executionmode", "once");

                if (priority > 0) {
                    future = this.priorityExecutor.schedule(runnableTask, delay, TimeUnit.SECONDS);
                } else {
                    future = executorService.schedule(runnableTask, delay, TimeUnit.SECONDS);
                }

                Logger.log("Job >" + job.getInt("id") + "< was scheduled one Time. Delay: " + delay);
            } else {
                returnJson.add("executionmode", "repeating");
                returnJson.add("executiondelay", delay);

                if (priority > 0) {
                    future = this.priorityExecutor.scheduleAtFixedRate(runnableTask, delay, repeatsecs, TimeUnit.SECONDS);
                } else {
                    future = executorService.scheduleWithFixedDelay(runnableTask, delay, repeatsecs, TimeUnit.SECONDS);
                }

                Logger.log("Job >" + job.getInt("id") + "< was scheduled with fixed rate. Delay: " + delay + " repeatsecs: " + repeatsecs);
            }

            String jobCode = this.getJobCode(smartdataurl, collection, storage, jobid);
            this.futures.put(jobCode, future);
        } catch (SmartDataAccessorException ex) {
            returnJson.add("state", "failed");
            returnJson.add("error", "Error accessing job actions: " + JSONFormatter.escapeForJson(ex.getLocalizedMessage()));
        } catch (JobRunnerException ex) {
            returnJson.add("state", "failed");
            returnJson.add("error", "Error accessing job actions: " + JSONFormatter.escapeForJson(ex.getLocalizedMessage()));
        }
        JsonObject jobuild = returnJson.build();
        return jobuild;
    }

    /**
     * Exeutes a single job (old signature kept for startAll)
     */
    private JsonObject executeJob(String smartdataurl, String collection, String storage, JsonObject job, SmartDataAccessor acc, JsonObject preJobInfo) {
        return this.executeJob(smartdataurl, collection, storage, job, acc, preJobInfo, null);
    }

    /**
     * Opens a new root run: fresh runid that is also the chainrunid, no
     * parent, step 0.
     *
     * @return Freshly initialised run record
     */
    private RunRecord newRootRun() {
        RunRecord run = new RunRecord();
        UUID runid = UUID.randomUUID();
        run.setRunid(runid);
        run.setChainrunid(runid);
        run.setParentrunid(null);
        run.setStep(0);
        return run;
    }

    /**
     * Computes the SHA-256 hex digest of the run input snapshot. Used as a
     * cheap equality check between runs (replay-fields.md §3.2 inputhash).
     */
    private String inputHash(JsonObject inputSnapshot) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(inputSnapshot.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException ex) {
            // Every JVM must provide SHA-256; fall back to an unhashed marker
            return "";
        }
    }

    /**
     * Calculates the seconds until next start
     *
     * @param start Configured start time
     * @return
     */
    private long calculateDelay(LocalDateTime start, long seconds) {
        LocalDateTime now = LocalDateTime.now();
        Duration sec = Duration.ofSeconds(seconds);

        // Prüfen, ob start vor now liegt
        if (start.isBefore(now)) {
            if (seconds != 0) {
                // Berechne die Zeitdifferenz zwischen start und now in Sekunden
                long elapsedSeconds = Duration.between(start, now).getSeconds();

                // Berechne die nächste start-Zeit in der Zukunft
                long missedIntervals = (elapsedSeconds + sec.getSeconds() - 1) / sec.getSeconds(); // Aufrunden
                start = start.plusSeconds(missedIntervals * sec.getSeconds());
            }
        }

        return now.until(start, ChronoUnit.SECONDS);
    }

    /**
     * Saves the result in database
     *
     * @param smartdataurl URL to the SmartData Instance that contains job
     * informations
     * @param collection Name of the collection contianing job informations
     * @param storage Storage containing job informations
     * @param datajob_id datajobs id
     * @param result JsonObject with results
     * @param run Run bookkeeping for the new log columns
     * @param haserror If an error was detected
     * @param wastimeout If an timeout occured
     */
    private void saveResultLog(String smartdataurl, String collection, String storage, Long datajob_id, JsonObject result, RunRecord run, boolean haserror, boolean wastimeout) throws JobRunnerException {
        Logger.log();
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        try {
            acc.saveRunLog(storage, datajob_id, result, run, haserror, wastimeout);
        } catch (SmartDataAccessorException ex) {
            JobRunnerException jrex = new JobRunnerException(ex.getLocalizedMessage());
            jrex.initCause(ex);
            throw jrex;
        }
    }

    // Old signature kept for transition: logs without run bookkeeping
    private void saveResultLog(String smartdataurl, String collection, String storage, Long datajob_id, JsonObject result, boolean haserror, boolean wastimeout) throws JobRunnerException {
        this.saveResultLog(smartdataurl, collection, storage, datajob_id, result, null, haserror, wastimeout);
    }

    /**
     * Gets and starts a jobchain from a job with its result
     *
     * @param smartdataurl URL to the SmartData Instance that contains job
     * informations
     * @param collection Name of the collection contianing job informations
     * @param storage Storage containing job informations
     * @param jobinfo Jobs information including a "result" object
     * @return List of chained jobs and their results
     * @throws JobRunnerException
     */
    public JsonArrayBuilder startJobChain(String smartdataurl, String collection, String storage, JsonObject jobinfo) throws JobRunnerException {
        return this.startJobChain(smartdataurl, collection, storage, jobinfo, null);
    }

    /**
     * Gets and starts a jobchain from a job with its result. When the run
     * identities of the triggering run are given, successor runs inherit
     * chainrunid and derive parentrunid/step from it (replay-fields.md G2).
     *
     * @param parentrun Run record of the triggering run; null = unknown (old
     * callers)
     * @param chainEdge The datajobs_chain row of the triggered edge; may be
     * null (old callers), then chain settings default
     */
    private JsonArrayBuilder startJobChain(String smartdataurl, String collection, String storage, JsonObject jobinfo, RunRecord parentrun) throws JobRunnerException {
        Logger.log();
        JsonArrayBuilder results = Json.createArrayBuilder();

        JsonNumber pre_job_id_json = jobinfo.getJsonNumber("id");
        if (pre_job_id_json == null) {
            throw new JobRunnerException("Could not start job chain: Id of previous job is missing.");
        }
        Long pre_job_id = pre_job_id_json.longValue();

        // The chain run continues into successors: a successor inherits the
        // chainrunid and becomes parent-linked to the triggering run.
        UUID chainrunid = parentrun != null ? parentrun.getChainrunid() : null;
        UUID parentrunid = parentrun != null ? parentrun.getRunid() : null;
        int parentStep = parentrun != null ? parentrun.getStep() : 0;

        // Get job chain for job
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        try {
            JsonObjectBuilder result = Json.createObjectBuilder();
            // Jobs array from database
            JsonArray chainedjobs = acc.fetchData(storage, collection + "_chain", null, "pre_job_id,eq," + pre_job_id, null, null, null, null, null, result);
            for (int i = 0; i < chainedjobs.size(); i++) {
                JsonObject jobparam = chainedjobs.getJsonObject(i);
                Long post_job_id = jobparam.getJsonNumber("post_job_id").longValue();
                if (!jobparam.getBoolean("active")) {
                    results.add("Did not execute chained job >" + post_job_id + "< because chain element is deactivated.");
                    continue;
                }
                String checkkey = jobparam.getString("checkkey", null);
                String checkvalue = jobparam.getString("checkvalue", null);
                String checkchanged = jobparam.getString("checkchanged", null);
                String onerror = jobparam.getString("onerror", "skip");
                Logger.log("Checking checkkey >" + checkkey + "< for checkvalue >" + checkvalue + "< and checkchanged >" + checkchanged + "<");

                // --- onerror policy (replay-fields.md 3.3) ---------------
                // skip (default) keeps today's behaviour: the configured
                // condition (checkkey/checkvalue or none) decides whether the
                // successor runs - an edge may intentionally react to a
                // failed state. Only onerror=fail actively stops the chain run
                // when the predecessor failed; onerror=run forces the successor
                // to start despite the failure (compensation/cleanup job).
                JsonObject resultObj = jobinfo.getJsonObject("result");
                boolean preFailed = resultObj != null
                        && ("failed".equals(resultObj.getString("state", ""))
                        || "error".equals(resultObj.getString("state", "")));
                if (preFailed && "fail".equals(onerror)) {
                    Logger.log("Did not start chained job >" + post_job_id + "< and stopped chain run because onerror=fail (predecessor failed).");
                    results.add("Did not execute chained job >" + post_job_id + "< because predecessor failed and onerror=fail stops the chain run.");
                    return results; // stop the whole chain run
                }
                boolean forceStartOnError = preFailed && "run".equals(onerror);

                // Check condition
                if (!forceStartOnError && checkkey != null) {
                    if (resultObj == null || !resultObj.containsKey(checkkey)) {
                        // replay-fields.md §6.4: this was a SILENT no-op. Report it consistently.
                        Logger.log("Did not start chained job >" + post_job_id + "< because checkkey >" + checkkey + "< is not part of the result.");
                        results.add("Did not execute chained job >" + post_job_id + "< because checkkey >" + checkkey + "< is not part of the predecessors result.");
                        continue;
                    }
                    JsonValue resultValue = resultObj.get(checkkey);
                    if (resultValue != null && resultValue.toString().replace("\"", "").equals(checkvalue)) {
                        // Check if state has changed
                        if (checkchanged != null && !this.checkChanged(smartdataurl, collection, storage, checkchanged, jobinfo, parentrunid)) {
                            Logger.log("Did not execute chained job >" + post_job_id + "< because result >" + checkchanged + "< has not changed.");
                            results.add("Did not execute chained job >" + post_job_id + "< because result >" + checkchanged + "< has not changed.");
                            continue;
                        }
                        Logger.log("Starting chained job >" + post_job_id + "< because of checkkey and statechange");
                        this.startSuccessor(smartdataurl, collection, storage, post_job_id, jobinfo, chainrunid, parentrunid, parentStep, jobparam, results);
                    } else {
                        Logger.log("Did not start chained job >" + post_job_id + "< because result >" + checkkey + "< has not expected value >" + checkvalue + "< but >" + resultValue.toString() + "<.");
                        results.add("Did not execute chained job >" + post_job_id + "< because result >" + checkkey + "< has not expected value >" + checkvalue + "< but >" + resultValue.toString() + "<.");
                    }
                } // Check state change
                else if (!forceStartOnError && checkchanged != null) {
                    // Check if state has changed
                    if (this.checkChanged(smartdataurl, collection, storage, checkchanged, jobinfo, parentrunid)) {
                        Logger.log("Starting chained job >" + post_job_id + "< because of statechange");
                        this.startSuccessor(smartdataurl, collection, storage, post_job_id, jobinfo, chainrunid, parentrunid, parentStep, jobparam, results);
                    } else {
                        Logger.log("Did not execute chained job >" + post_job_id + "< because result >" + checkchanged + "< has not changed.");
                        results.add("Did not execute chained job >" + post_job_id + "< because result >" + checkchanged + "< has not changed.");
                    }
                } else {
                    if (forceStartOnError) {
                        Logger.log("Starting chained job >" + post_job_id + "< because onerror=run (predecessor failed).");
                        this.startSuccessor(smartdataurl, collection, storage, post_job_id, jobinfo, chainrunid, parentrunid, parentStep, jobparam, results);
                        results.add("Executed chained job >" + post_job_id + "< because onerror=run overrides the condition after a predecessor failure.");
                    } else {
                        Logger.log("Starting chained job >" + post_job_id + "< without dependency in checkkey and statechange");
                        this.startSuccessor(smartdataurl, collection, storage, post_job_id, jobinfo, chainrunid, parentrunid, parentStep, jobparam, results);
                        results.add("Executed chained job >" + post_job_id + "< because there is no condition configured.");
                    }
                }
            }
        } catch (SmartDataAccessorException ex) {
            JobRunnerException jrex = new JobRunnerException(ex.getLocalizedMessage());
            jrex.initCause(ex);
            throw jrex;
        }
        return results;
    }

    /**
     * Starts a successor job of a chain run with full lineage: inherits the
     * chainrunid, parent-links to the triggering run and increments the step.
     * Applies the maxchaindepth guard of the successor job definition
     * (replay-fields.md §3.1).
     */
    private void startSuccessor(String smartdataurl, String collection, String storage, long post_job_id, JsonObject jobinfo, UUID chainrunid, UUID parentrunid, int parentStep, JsonObject chainEdge, JsonArrayBuilder results) throws JobRunnerException {
        // Load successor job definition for the maxchaindepth guard
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        try {
            JsonObjectBuilder fetchResult = Json.createObjectBuilder();
            JsonArray postjobs = acc.fetchData(storage, collection, null, "id,eq," + post_job_id, null, null, null, null, null, fetchResult);
            if (postjobs.isEmpty()) {
                results.add("Did not execute chained job >" + post_job_id + "< because its definition could not be loaded.");
                return;
            }
            JsonObject postjob = postjobs.getJsonObject(0);
            int maxchaindepth = postjob.getInt("maxchaindepth", 0);
            if (maxchaindepth > 0 && parentStep + 1 > maxchaindepth) {
                Logger.log("Did not start chained job >" + post_job_id + "< because chain depth >" + (parentStep + 1) + "< exceeds maxchaindepth >" + maxchaindepth + "< of the job.");
                results.add("Did not execute chained job >" + post_job_id + "< because the maximum chain depth >" + maxchaindepth + "< would be exceeded.");
                return;
            }

            // Compose the successor run record
            RunRecord successorRun = new RunRecord();
            UUID runid = UUID.randomUUID();
            successorRun.setRunid(runid);
            successorRun.setChainrunid(chainrunid);
            successorRun.setParentrunid(parentrunid);
            successorRun.setStep(parentStep + 1);

            // Attempt counter within this chain run: number of prior runs of
            // this job in the same chain run + 1. Computed from the log.
            successorRun.setAttempt(this.countAttempts(smartdataurl, collection, storage, post_job_id, chainrunid) + 1);

            JsonArray chainjobinfos = this.start(smartdataurl, collection, storage, post_job_id, jobinfo, successorRun);
            results.add(chainjobinfos);
        } catch (SmartDataAccessorException ex) {
            JobRunnerException jrex = new JobRunnerException(ex.getLocalizedMessage());
            jrex.initCause(ex);
            throw jrex;
        }
    }

    /**
     * Counts the runs a job already had within one chain run (for the
     * attempt counter). Returns 0 when the log cannot be read; the attempt
     * number is diagnostic, not load-bearing, so a read failure must not
     * abort the chain.
     */
    private int countAttempts(String smartdataurl, String collection, String storage, Long jobid, UUID chainrunid) {
        if (chainrunid == null) {
            return 0;
        }
        try {
            SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
            JsonArray runs = acc.fetchRunsOfChain(storage, chainrunid);
            int count = 0;
            for (JsonObject run : runs.getValuesAs(JsonObject.class)) {
                if (run.getJsonNumber("datajob_id") != null && run.getJsonNumber("datajob_id").longValue() == jobid) {
                    count++;
                }
            }
            return count;
        } catch (Exception ex) {
            Logger.log("Could not count attempts for job >" + jobid + "<: " + ex.getLocalizedMessage());
            return 0;
        }
    }

    /**
     * Check if a field has changed in compare to the run of the job that
     * triggered this chain hop (replay-fields.md 6.2).
     *
     * When the triggering run's identity is known (parentrunid), the exact
     * predecessor log row is compared. Otherwise the previous row of the
     * newest two log rows is used as fallback (old behaviour, kept for
     * callers without run identities).
     */
    private boolean checkChanged(String smartdataurl, String collection, String storage, String checkchanged, JsonObject jobinfo, UUID parentrunid) throws SmartDataAccessorException {
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        JsonObjectBuilder result = Json.createObjectBuilder();
        JsonArray jobresults;
        if (parentrunid != null) {
            // Exact comparison against the triggering run (replay-fields.md 6.2)
            jobresults = acc.fetchData(storage, collection + "_log", null, "runid,eq," + parentrunid.toString(), null, null, null, "ts,DESC", 1L, result);
            if (jobresults.isEmpty()) {
                Logger.log("There was no run with runid >" + parentrunid + "<. So: Attribute >" + checkchanged + "< has changed.");
                return true;
            }
            String lastvalue = parseResultAttribute(jobresults.getJsonObject(0), checkchanged);
            String curvalue = jobinfo.getJsonObject("result").getString(checkchanged, null);
            if (curvalue == null) {
                Logger.log("Current result has no attribute >" + checkchanged + "<. Attribute has changed.");
                return true;
            }
            if (curvalue.equals(lastvalue)) {
                Logger.log("Attribute >" + checkchanged + "< has not changed from value >" + lastvalue + "<");
                return false;
            }
            Logger.log("Attribute >" + checkchanged + "< has changed from value >" + lastvalue + "< to value >" + curvalue + "<");
            return true;
        } else {
            jobresults = acc.fetchData(storage, collection + "_log", null, "datajob_id,eq," + jobinfo.getInt("id"), null, null, null, "ts,DESC", 2L, result);
            if (!jobresults.isEmpty() && jobresults.size() > 1) {
                String lastvalue = parseResultAttribute(jobresults.getJsonObject(1), checkchanged);
                String curvalue = jobinfo.getJsonObject("result").getString(checkchanged, null);
                if (curvalue == null || curvalue.equals(lastvalue)) {
                    Logger.log("Attribute >" + checkchanged + "< has not changed from value >" + lastvalue + "<");
                    return false;
                }
                Logger.log("Attribute >" + checkchanged + "< has changed from value >" + lastvalue + "< to value >" + curvalue + "<");
                return true;
            }
            Logger.log("There was no previous execution. So: Attribute >" + checkchanged + "< has changed.");
            return true;
        }
    }

    /**
     * Extracts an attribute from the result JSON stored in a datajobs_log row.
     * Returns null when the row or the attribute does not exist or the result
     * is not valid JSON.
     */
    private String parseResultAttribute(JsonObject logrow, String attribute) {
        String resultstr = logrow.getString("result", null);
        if (resultstr == null) {
            return null;
        }
        try (JsonReader jsonReader = Json.createReader(new StringReader(resultstr))) {
            JsonObject jobresultobj = jsonReader.readObject();
            return jobresultobj.getString(attribute, null);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    public void shutdownSmartDataJobs() {
        this.stopAllJobs();

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (priorityExecutor != null) {
            priorityExecutor.shutdown();
            try {
                if (!priorityExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    priorityExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                priorityExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Stops the execution of all jobs and schedules
     *
     * @return State information
     */
    public JsonObject stopAllJobs() {
        Logger.log("Shutting down schedules jobs...");
        JsonObjectBuilder result = Json.createObjectBuilder();

        for (Entry<String, Future<?>> future : futures.entrySet()) {
            if (!future.getValue().isDone()) {
                future.getValue().cancel(true);
                result.add("Job >" + future.getKey() + "<", "stopped");
            }
        }
        this.futures = new java.util.concurrent.ConcurrentHashMap<>();

        result.add("state", "stopped");
        result.add("statemsg", "All running jobs are gracefully stopped.");
        return result.build();
    }

    /**
     * Shutdown a single job
     */
    public JsonObject stopJob(String smartdataurl, String collection, String storage, Long job_id) {
        return this.stopJob(smartdataurl, collection, storage, job_id, null);
    }

    /**
     * Shutdown a single job. The run record (or a fresh root run, when none is
     * given) marks the stop log entry as runstate=stopped
     * (replay-fields.md 3.2 vocabulary).
     */
    public JsonObject stopJob(String smartdataurl, String collection, String storage, Long job_id, RunRecord run) {
        JsonObjectBuilder result = Json.createObjectBuilder();
        String jobCode = this.getJobCode(smartdataurl, collection, storage, job_id);
        for (Entry<String, Future<?>> future : futures.entrySet()) {
            if (future.getKey().equals(jobCode)) {
                if (!future.getValue().isDone()) {
                    JsonObject stoplog = Json.createObjectBuilder()
                            .add("state", "stopped")
                            .add("stateMsg", "Job execution was stopped.").build();
                    RunRecord stoplogRun = run != null ? run : this.newRootRun();
                    stoplogRun.setRunstate("stopped");
                    try {
                        saveResultLog(smartdataurl, collection, storage, job_id, stoplog, stoplogRun, false, false);
                    } catch (JobRunnerException ex) {
                        result.add("error", "Could not save stop in log: " + JSONFormatter.escapeForJson(ex.getLocalizedMessage()));
                    }
                    if (executingJobs.contains(jobCode)) {
                        stoppedWhileExecuting.add(jobCode);
                    }
                    future.getValue().cancel(true);
                }
                futures.remove(future.getKey());
                result.add("Job >" + future.getKey() + "<", "stopped");
            }
        }
        result.add("state", "stopped");
        return result.build();
    }

    /**
     * Checks if a job is running
     *
     * @return true if job with id is running, false otherwise
     */
    public boolean jobIsRunning(String smartdataurl, String collection, String storage, Long job_id) {
        boolean isRunning = false;
        String jobCode = this.getJobCode(smartdataurl, collection, storage, job_id);
        for (Entry<String, Future<?>> future : futures.entrySet()) {
            if (future.getKey().equals(jobCode)) {
                if (!future.getValue().isDone()) {
                    isRunning = true;
                }
            }
        }
        return isRunning;
    }

    /**
     * Generates a job identifier (id is not enough because jobs can be defined
     * in more instances of SmartData)
     */
    public String getJobCode(String smartdataurl, String collection, String storage, Long job_id) {
        return smartdataurl + collection + storage + job_id;
    }

    /**
     * Lists the states of all jobs
     *
     * @return JsonArray with one entry for each job
     */
    public JsonArray getJobStates(String smartdataurl, String collection, String storage) {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        JsonArray jobs;
        try {
            jobs = acc.fetchData(storage, collection, "id", null, null, null, null, null, null, Json.createObjectBuilder());
        } catch (SmartDataAccessorException ex) {
            jab.add(Json.createObjectBuilder().add("error", "Could not fetch jobs: " + JSONFormatter.escapeForJson(ex.getLocalizedMessage())));
            return jab.build();
        }

        for (JsonObject curJob : jobs.getValuesAs(JsonObject.class)) {
            JsonObjectBuilder job = Json.createObjectBuilder();
            long jobid = curJob.getJsonNumber("id").longValue();
            job.add("id", jobid);

            // Get actual state
            String jobCode = this.getJobCode(smartdataurl, collection, storage, jobid);
            Future<?> future = futures.get(jobCode);
            if (executingJobs.contains(jobCode)) {
                job.add("state", "running");
            } else if (future == null || future.isDone()) {
                job.add("state", "stopped");
            } else {
                job.add("state", "waiting");
            }

            // Get last activity log
            try {
                JsonObjectBuilder result = Json.createObjectBuilder();
                JsonArray data = acc.fetchData(storage, collection + "_log", null, "datajob_id,eq," + jobid, "ts", null, null, "ts,DESC", 1L, result);
                job.add("lastlog", data);
            } catch (SmartDataAccessorException ex) {
                job.add("error", "Could not fetch last activity: " + JSONFormatter.escapeForJson(ex.getLocalizedMessage()));
            }
            jab.add(job);
        }

        return jab.build();
    }

    public JsonObject setActiveOfJob(String smartdataurl, String collection, String storage, Long job_id, boolean active) {
        JsonObjectBuilder result = Json.createObjectBuilder();
        JsonObject updateJob = Json.createObjectBuilder()
                .add("id", job_id)
                .add("active", active).build();
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        result.add("job_id", job_id);
        try {
            acc.updateData(collection, storage, updateJob);
            result.add("status", "updated")
                    .add("active", active);
        } catch (SmartDataAccessorException ex) {
            result.add("error", "Could not update column active to >" + active + "< of Job >" + job_id + "<");
        }
        return result.build();
    }

    /**
     * Builds the replay plan for one chain run (replay-fields.md 5.1, dry run
     * only). Loads the chain run, determines the resume point and the runs
     * that would re-execute. Read-only: no job is started, nothing written.
     *
     * @param chainrunid Chain run to plan the resume for
     * @param fromrunid Explicit resume point inside the chain run; null =
     * first run not finished/replayed
     * @return Plan object with all runs, the resume point and the actions
     */
    public JsonObject replayPlan(String smartdataurl, String collection, String storage, UUID chainrunid, UUID fromrunid) throws JobRunnerException {
        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        try {
            JsonArray runs = acc.fetchRunsOfChain(storage, chainrunid);
            JsonObjectBuilder plan = Json.createObjectBuilder();
            plan.add("chainrunid", chainrunid.toString());
            plan.add("runs", runs);

            // Determine resume point: the given one, or the first run that is
            // not finished/replayed
            UUID resumePoint = fromrunid;
            if (resumePoint == null) {
                for (JsonObject curRun : runs.getValuesAs(JsonObject.class)) {
                    String runstate = curRun.getString("runstate", "");
                    if (!"finished".equals(runstate) && !"replayed".equals(runstate) && curRun.containsKey("runid")) {
                        resumePoint = UUID.fromString(curRun.getString("runid"));
                        break;
                    }
                }
            }
            if (resumePoint == null) {
                plan.add("state", "nothing to resume");
                plan.add("plan", Json.createArrayBuilder().build());
                return plan.build();
            }
            plan.add("resumePointRunId", resumePoint.toString());
            plan.add("resumePointGiven", fromrunid != null);

            // Steps at or after the resume point re-execute (unless
            // finished/replayed later steps stay untouched unless force - the
            // plan reports which). Steps before the resume point are skipped
            // when they finished; runs with unknown outcome re-execute.
            int resumeStep = this.stepOfRun(runs, resumePoint);
            JsonArrayBuilder planSteps = Json.createArrayBuilder();
            for (JsonObject curRun : runs.getValuesAs(JsonObject.class)) {
                JsonObjectBuilder planStep = Json.createObjectBuilder();
                if (curRun.containsKey("runid")) {
                    planStep.add("runid", curRun.getString("runid"));
                }
                if (curRun.containsKey("datajob_id")) {
                    planStep.add("datajob_id", curRun.getJsonNumber("datajob_id"));
                }
                int step = curRun.getInt("step", 0);
                planStep.add("step", step);
                String runstate = curRun.getString("runstate", null);
                planStep.add("runstate", runstate != null ? runstate : "unknown");

                String action;
                String curRunid = curRun.getString("runid", null);
                if (curRunid != null && curRunid.equals(resumePoint.toString())) {
                    action = "re-execute";
                } else if (step >= resumeStep) {
                    if ("finished".equals(runstate) || "replayed".equals(runstate)) {
                        action = "re-execute (finished; force required to re-run)";
                    } else {
                        action = "re-execute";
                    }
                } else if ("finished".equals(runstate) || "replayed".equals(runstate)) {
                    action = "skip";
                } else {
                    action = "re-execute (before resume point, unfinished)";
                }
                planStep.add("action", action);
                if (curRun.containsKey("input")) {
                    planStep.add("inputSnapshotAvailable", true);
                }
                planSteps.add(planStep);
            }
            plan.add("plan", planSteps);
            return plan.build();
        } catch (SmartDataAccessorException ex) {
            JobRunnerException jrex = new JobRunnerException(ex.getLocalizedMessage());
            jrex.initCause(ex);
            throw jrex;
        }
    }

    /**
     * Gets the step number of the run with the given runid inside the runs
     * array. Returns -1 when the run is not part of the array.
     */
    private int stepOfRun(JsonArray runs, UUID runid) {
        for (JsonObject curRun : runs.getValuesAs(JsonObject.class)) {
            String curRunid = curRun.getString("runid", null);
            if (curRunid != null && curRunid.equals(runid.toString())) {
                return curRun.getInt("step", 0);
            }
        }
        return -1;
    }

    private Action getJobAction(JsonObject job) {
        Action jobsaction;
        String actionname = job.getString("action", null);
        if (actionname == null) {
            return null;
        }
        switch (actionname) {
            case "CheckNet":
                jobsaction = new CheckNetAction();
                break;
            case "Mail":
                jobsaction = new MailAction();
                break;
            case "CommandExecution":
                jobsaction = new CommandExecutionAction();
                break;
            case "ReadData":
                jobsaction = new ReadDataAction();
                break;
            case "WriteData":
                jobsaction = new WriteDataAction();
                break;
            case "SendNotification":
                jobsaction = new SendNotificationAction();
                break;
            case "CheckEventTriggers":
                jobsaction = new CheckEventTriggers();
                break;
            case "CreateFlagFile":
                jobsaction = new CreateFlagFileAction();
                break;
            case "DeleteData":
                jobsaction = new DeleteDataAction();
                break;
            default:
                jobsaction = null;
        }
        return jobsaction;
    }

    /**
     * Loads the job parameters from the fetched datajobs_params records.
     *
     * @param jobparams Fetched parameter records
     * @param returnJson Builder for the job result; gets a "paramError" entry
     * for every parameter that could not be loaded
     * @return JsonObject with the loaded parameters, or null if at least one
     * parameter could not be loaded
     */
    private JsonObject loadJobParams(JsonArray jobparams, JsonObjectBuilder returnJson) {
        // Transform to object
        JsonObjectBuilder jobparamobjb = Json.createObjectBuilder();
        // Every parameter problem is collected in paramErrors (replay-fields.md
        // 6.1: a single paramError string was overwritten by each subsequent
        // problem, so only the last diagnostic survived). paramError keeps the
        // last message for backwards compatibility.
        JsonArrayBuilder paramErrors = Json.createArrayBuilder();
        boolean paramerror = false;
        for (int i = 0; i < jobparams.size(); i++) {
            JsonObject jobparam = jobparams.getJsonObject(i);
            try {
                String key = jobparam.getString("key");
                String valstr = jobparam.getString("value");
                String type = jobparam.getString("type", null);
                if (type == null) {
                    String msg = "Parameter type is missing for configuration entry >" + key + "< with value >" + valstr + "<. Please use string, email, url, int or float.";
                    paramErrors.add(msg);
                    returnJson.add("paramError", msg);
                    paramerror = true;
                    continue;
                }
                switch (type) {
                    case "string":
                    case "email":
                    case "url":
                        jobparamobjb.add(key, valstr);
                        break;
                    case "int":
                        jobparamobjb.add(key, Integer.parseInt(valstr));
                        break;
                    case "float":
                        jobparamobjb.add(key, Float.parseFloat(valstr));
                        break;
                    default:
                        String msg = "Parameter type >" + type + "< for configuration entry >" + key + "< with value >" + valstr + "< is not known. Please use string, email, url, int or float.";
                        paramErrors.add(msg);
                        returnJson.add("paramError", msg);
                        paramerror = true;
                }
            } catch (RuntimeException ex) {
                // A single malformed parameter must not abort loading the remaining parameters
                String msg = "Could not load parameter >" + jobparam.getString("key", "unknown") + "<: " + ex.getClass().getSimpleName() + ": " + JSONFormatter.escapeForJson(ex.getLocalizedMessage());
                paramErrors.add(msg);
                returnJson.add("paramError", msg);
                paramerror = true;
            }
        }
        if (paramerror) {
            returnJson.add("paramErrors", paramErrors);
            return null;
        }
        return jobparamobjb.build();
    }

    private void runJobAsync(Action jobsaction, String smartdataurl, String collection, String storage, JsonObject job, long jobid, LocalDateTime stop, JsonObject params) {
        this.runJobAsync(jobsaction, smartdataurl, collection, storage, job, jobid, stop, params, null);
    }

    /**
     * Executes the action asynchronously, writes the run log with the full
     * run bookkeeping and continues the chain with run lineage.
     */
    private void runJobAsync(Action jobsaction, String smartdataurl, String collection, String storage, JsonObject job, long jobid, LocalDateTime stop, JsonObject params, RunRecord run) {
        Logger.startProcess(jobid + " (" + job.getString("name") + "):" + Instant.now().toString());
        Logger.log("Now running runnableTask for job >" + jobid + "<");
        String jobCode = this.getJobCode(smartdataurl, collection, storage, jobid);
        executingJobs.add(jobCode);

        // Every invocation needs its own run identity. The record handed
        // over from executeJob describes the *scheduling*; a repeating
        // schedule reuses the runnable, and per replay-fields.md 8 (Q1)
        // every tick is its own chain run. So a record without parent lineage
        // is only used as the template: each invocation mints a fresh root
        // run and carries over the input snapshot. A successor run
        // (parentrunid set) is executed exactly once with its given identity.
        final RunRecord currun;
        if (run != null && run.getParentrunid() != null) {
            currun = run;
        } else {
            currun = this.newRootRun();
            if (run != null) {
                currun.setInput(run.getInput());
                currun.setInputhash(run.getInputhash());
                currun.setAttempt(run.getAttempt());
            }
        }

        try {
            if (stopped) {
                JsonObjectBuilder result = Json.createObjectBuilder();
                result.add("state", "stopped");
                result.add("stateMsg", "Job execution stopped, because SmartDataJobs was stopped.");
                currun.setRunstate("stopped");
                saveResultLog(smartdataurl, collection, storage, jobid, result.build(), currun, false, false);
                return;
            }

            // Stop time check
            if (stop != null && LocalDateTime.now().isAfter(stop)) {
                Logger.log("Job >" + jobid + "< stopped because stop-time reached.");
                this.stopJob(smartdataurl, collection, storage, jobid, currun);
                return;
            }

            currun.setRunstate("running");
            long startTime = System.currentTimeMillis();
            JsonObject result;
            try {
                result = jobsaction.call();
            } catch (Exception actionEx) {
                // The action threw instead of returning a result object.
                // Classify as error run with full info in the result.
                result = Json.createObjectBuilder()
                        .add("state", "error")
                        .add("error", actionEx.getClass().getSimpleName() + " occured: " + JSONFormatter.escapeForJson(actionEx.getLocalizedMessage()))
                        .build();
            }
            currun.setDurationms(System.currentTimeMillis() - startTime);

            JsonObjectBuilder innerJobinfo = Json.createObjectBuilder();
            job.forEach(innerJobinfo::add);
            innerJobinfo.add("params", params);
            innerJobinfo.add("result", result);

            // Derive the error flags from the action's own verdict instead of hardcoding them
            String state = result.getString("state", "");
            String statemsg = result.containsKey("statemsg")
                    ? result.getString("statemsg")
                    : result.getString("stateMsg", null);
            boolean haserror = "failed".equals(state) || "error".equals(state);
            boolean wastimeout = statemsg != null && statemsg.toLowerCase().contains("timeout");

            currun.setRunstate(this.deriveRunstate(wastimeout, haserror));
            currun.setErrorclass(this.deriveErrorclass(statemsg, result, haserror));
            JsonObject innerJobinfoBuilt = innerJobinfo.build();

            // Materialise the result validity deadline from the jobs TTL policy
            // (replay-fields.md 9.4): resultvaliduntil = now + resultttlsecs.
            // The job row is already at hand (captured with the runnable), no
            // second fetch needed. TTL 0 = never reuse, -1 = forever: only a
            // positive ttl gets an expiry.
            try {
                int ttl = job.getInt("resultttlsecs", 0);
                if (ttl > 0) {
                    currun.setResultvaliduntil(OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(ttl));
                }
            } catch (RuntimeException ttlex) {
                // TTL is an optional policy; a malformed value must not fail the run
                Logger.log("Could not apply result TTL for job >" + jobid + "<: " + ttlex.getLocalizedMessage());
            }

            saveResultLog(smartdataurl, collection, storage, jobid, result, currun, haserror, wastimeout);
            if (stoppedWhileExecuting.contains(jobCode)) {
                Logger.log("Job >" + jobid + "< was stopped, chained jobs are not started.");
                return;
            }
            final RunRecord chainParentRun = currun;
            CompletableFuture.runAsync(() -> {
                try {
                    this.startJobChain(smartdataurl, collection, storage, innerJobinfoBuilt, chainParentRun);
                } catch (Exception e) {
                    Logger.log("Error while starting jobChain for job >" + jobid + "<: " + e.getLocalizedMessage());
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            JsonObjectBuilder result = Json.createObjectBuilder();
            result.add("state", "error");
            result.add("error", e.getClass().getSimpleName() + " occured: " + JSONFormatter.escapeForJson(e.getLocalizedMessage()));
            currun.setRunstate("error");
            currun.setErrorclass("internal");
            // saveResultLog can itself throw (DB hiccup). Logging that must
            // not kill the repeating schedule (replay-fields.md 6.5): catch
            // here instead of rethrowing into the scheduled task.
            try {
                saveResultLog(smartdataurl, collection, storage, jobid, result.build(), currun, true, false);
            } catch (JobRunnerException ex) {
                Logger.log("Execution was done. But result could not be saved: " + ex.getLocalizedMessage());
            }
            Logger.log("Error occured while executing job >" + jobid + "<: " + e.getLocalizedMessage());
            e.printStackTrace();
        } finally {
            executingJobs.remove(jobCode);
            stoppedWhileExecuting.remove(jobCode);
            Logger.endProcess();
        }
    }

    /**
     * Maps the action verdict to the normalised runstate vocabulary
     * (replay-fields.md 3.2):
     * scheduled|running|finished|failed|error|timeout|stopped|skipped|replayed
     *
     * Keyed on the canonical error decision (iserror) so the free-text state
     * can never disagree with the denormalised iserror column: every
     * non-error outcome - including the free-form positive states like
     * "connected" or "nothing todo" - collapses to finished.
     */
    private String deriveRunstate(boolean wastimeout, boolean haserror) {
        if (wastimeout) {
            return "timeout";
        }
        if (haserror) {
            return "failed";
        }
        return "finished";
    }

    /**
     * Derives the coarse failure taxonomy (replay-fields.md 3.2):
     * network|http_4xx|http_5xx|timeout|exec|misconfigured|dependency|internal
     *
     * Only meaningful for failed runs; returns null for successful runs
     * (the column stays empty).
     */
    private String deriveErrorclass(String statemsg, JsonObject result, boolean haserror) {
        if (!haserror) {
            return null;
        }
        // CheckNet reports the status under recivedStatusCode; the network
        // actions ReadData/WriteData use httpStatus.
        int httpStatus = -1;
        if (result.containsKey("httpStatus")) {
            httpStatus = result.getJsonNumber("httpStatus").intValue();
        } else if (result.containsKey("recivedStatusCode")) {
            httpStatus = result.getJsonNumber("recivedStatusCode").intValue();
        }
        String msg = statemsg != null ? statemsg.toLowerCase() : "";
        if (msg.contains("timeout")) {
            return "timeout";
        }
        if (httpStatus >= 400 && httpStatus < 500) {
            return "http_4xx";
        }
        if (httpStatus >= 500) {
            return "http_5xx";
        }
        if (msg.contains("dns") || msg.contains("connect") || msg.contains("unreachable") || msg.contains("unknownhost")) {
            return "network";
        }
        if (msg.contains("exit code") || msg.contains("exited with code") || msg.contains("no such file") || msg.contains("cannot run program")) {
            return "exec";
        }
        if (msg.contains("parameter") || msg.contains("misconfigur") || msg.contains("missing") || msg.contains("could not parse")) {
            return "misconfigured";
        }
        if (result.containsKey("exitcode") && result.getInt("exitcode", 0) != 0) {
            return "exec";
        }
        return "internal";
    }
}
