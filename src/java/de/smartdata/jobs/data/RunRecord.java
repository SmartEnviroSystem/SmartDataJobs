package de.smartdata.jobs.data;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Identity and bookkeeping of a single job run (replay-fields.md §3.2).
 *
 * One run corresponds to one row in the datajobs_log table. The runid is
 * minted by the runner (JobRunner), never by the database, so it can already
 * be referenced by child runs before the log row exists.
 *
 * Vocabulary of runstate:
 * scheduled|running|finished|failed|error|timeout|stopped|skipped|replayed
 *
 * Vocabulary of errorclass:
 * network|http_4xx|http_5xx|timeout|exec|misconfigured|dependency|internal
 */
public class RunRecord {

    private UUID runid;
    private UUID chainrunid;
    private UUID parentrunid;
    private int step = 0;
    private String runstate;
    private String input;
    private String inputhash;
    private String errorclass;
    private int attempt = 1;
    private UUID replayofrunid;
    private String replayreason;
    private OffsetDateTime resultvaliduntil;
    private long durationms = -1;

    public UUID getRunid() {
        return runid;
    }

    public void setRunid(UUID runid) {
        this.runid = runid;
    }

    public UUID getChainrunid() {
        return chainrunid;
    }

    public void setChainrunid(UUID chainrunid) {
        this.chainrunid = chainrunid;
    }

    public UUID getParentrunid() {
        return parentrunid;
    }

    public void setParentrunid(UUID parentrunid) {
        this.parentrunid = parentrunid;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getRunstate() {
        return runstate;
    }

    public void setRunstate(String runstate) {
        this.runstate = runstate;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getInputhash() {
        return inputhash;
    }

    public void setInputhash(String inputhash) {
        this.inputhash = inputhash;
    }

    public String getErrorclass() {
        return errorclass;
    }

    public void setErrorclass(String errorclass) {
        this.errorclass = errorclass;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public UUID getReplayofrunid() {
        return replayofrunid;
    }

    public void setReplayofrunid(UUID replayofrunid) {
        this.replayofrunid = replayofrunid;
    }

    public String getReplayreason() {
        return replayreason;
    }

    public void setReplayreason(String replayreason) {
        this.replayreason = replayreason;
    }

    public OffsetDateTime getResultvaliduntil() {
        return resultvaliduntil;
    }

    public void setResultvaliduntil(OffsetDateTime resultvaliduntil) {
        this.resultvaliduntil = resultvaliduntil;
    }

    /**
     * Duration of the action execution in milliseconds. -1 = unknown/not
     * measured (the caller did not reach the action call).
     *
     * @return duration in ms or -1
     */
    public long getDurationms() {
        return durationms;
    }

    public void setDurationms(long durationms) {
        this.durationms = durationms;
    }
}
