package de.smartdata.Remote;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RemotePythonExecutor {
    private  String name;
    private String password;
    private String host;
    private int port = 22;

    //Remote Path: remote_scripts

    public RemotePythonExecutor(String name, String password, String host){
        this.name = name;
        this.password = password;
        this.host = host;
    }

    public RemotePythonExecutor(String name, String password, String host, int port){
        this.name = name;
        this.password = password;
        this.host = host;
        this.port = port;
    }

    public void TestConnection(){
        try {
            SSHClient client = new SSHClient();
            client.addHostKeyVerifier(new PromiscuousVerifier());
            client.connect(host, port);
            client.authPassword(name, password);

            Session session = client.startSession();
            Session.Command cmd = session.exec("");

            BufferedReader reader = new BufferedReader(new InputStreamReader(cmd.getInputStream()));
            String line;
            while((line = reader.readLine()) != null){
                System.out.println(line);
            }
            cmd.join(5, TimeUnit.SECONDS);
            session.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean upload_SCP(String upload_path, String script_Path){

        try{
            SSHClient client = new SSHClient();
            client.addHostKeyVerifier(new PromiscuousVerifier()); //Gegen richtigen Schlüssel tauschen
            client.connect(host, port);
            client.authPassword(name, password);

            client.useCompression();
            client.newSCPFileTransfer().upload(new FileSystemFile(script_Path), upload_path);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    public boolean download_SCP(String remote_path, String local_path){

        try{
            SSHClient client = new SSHClient();
            client.addHostKeyVerifier(new PromiscuousVerifier());
            client.connect(host,port);
            client.authPassword(name,password);

            client.newSCPFileTransfer().download(local_path, remote_path);

        }catch (Exception e){
            throw new RuntimeException();
        }

        return true;
    }

    public String find(String scriptid, int scriptVersion){

        String search = "*" + scriptid + "*" + scriptVersion + "*.py";

        try{
            SSHClient client = new SSHClient();
            client.addHostKeyVerifier(new PromiscuousVerifier());
            client.connect(host, port);
            client.authPassword(name, password);

            Session session = client.startSession();
            Session.Command cmd = session.exec("dir " + search + "/s /b ");

            BufferedReader reader = new BufferedReader(new InputStreamReader(cmd.getInputStream()));
            String line;
            while((line = reader.readLine()) != null){
                System.out.println(line);
            }
            cmd.join(5, TimeUnit.SECONDS);

            session.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return "";
    }

    public String execute(String script, String params, String timeout){

        UUID runId = UUID.randomUUID();

        String execute_script = "python " + script;
        StringBuilder res = new StringBuilder();

        try{
            SSHClient client = new SSHClient();
            client.addHostKeyVerifier(new PromiscuousVerifier());
            client.connect(host, port);
            client.authPassword(name, password);

            Session session = client.startSession();
            Session.Command cmd = session.exec(execute_script);

            BufferedReader reader = new BufferedReader(new InputStreamReader(cmd.getInputStream()));
            String line;

            //Nur für den Test, ob etwas auf dem Terminal steht. Wird später entfernt
            while((line = reader.readLine()) != null){
                System.out.println(line);
                res.append(line);
            }

            cmd.join(5, TimeUnit.SECONDS);

            session.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return res.toString();
    }
}
