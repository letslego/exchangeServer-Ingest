#!/bin/env groovy
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import com.sun.jersey.api.client.filter.LoggingFilter
import com.sun.jersey.api.client.Client
import com.sun.jersey.core.util.MultivaluedMapImpl
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.lang.ArrayUtils
import org.apache.commons.lang.StringUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document;
import javax.mail.*
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import groovy.lang.Binding

import ProxyAuthenticator
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.CreatePipelineRequest;
import com.amazonaws.services.datapipeline.model.CreatePipelineResult;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionResult;
import com.amazonaws.services.datapipeline.model.ValidatePipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.ValidatePipelineDefinitionResult;
import com.amazonaws.services.datapipeline.model.ValidationError







def loadProcessedMessagesMap(file2) {
    System.out.println("Loading already processed email messages  list from file into memory")

    Map mp = [:];
    try {
        FileReader fw = new FileReader(file2.getAbsoluteFile());
        BufferedReader bw = new BufferedReader(fw);

        String line = bw.readLine();
        while (!StringUtils.isEmpty(line)) {
            mp.put(line, "")
            line = bw.readLine();
        }

        bw.close();
        fw.close();
        return mp;


    } catch (Exception e) {
        System.out.println("Error : Loading already processed email messages  list from file into memory,turning empty list")
        e.printStackTrace()
        return mp;
    }
}

def saveMessageStoreInDisk(outputFile2, mp) {
    try {
        System.out.println("saving processed msg Id s in file ")
        //save to a temporary file first
        File tempFile = new File("../store/prodEmail.txt");
        FileWriter fw1 = new FileWriter(tempFile.getAbsoluteFile(), true);
        BufferedWriter bw1 = new BufferedWriter(fw1);

        mp.each { msgId, value ->
            bw1.writeLine(msgId)

        }
        bw1.close();
        fw1.close()
        //delete content of the old file
        RandomAccessFile raf = new RandomAccessFile(outputFile2, "rw");
        raf.setLength(0);
        raf.close();
        //copy over into old file
        File oldFile = new File(outputFile2);
        FileWriter fw = new FileWriter(oldFile.getAbsoluteFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);

        mp.each { msgId, value ->
            bw.writeLine(msgId)
        }
        bw.close();
        fw.close()
        //delete content of the temp file
        RandomAccessFile raf1 = new RandomAccessFile("../store/prodEmail.txt", "rw");
        raf1.setLength(0);
        raf1.close();


    } catch (Exception e) {
        System.out.println("Error : saving processed msg Id s in file ")

        e.printStackTrace()
    }


}


def parseEmailContent(message) {
    String result;

    if (message instanceof MimeMessage) {
        MimeMessage m = (MimeMessage) message;
        Object contentObject = m.getContent();
        if (contentObject instanceof Multipart) {
            BodyPart clearTextPart = null;
            BodyPart htmlTextPart = null;
            Multipart content = (Multipart) contentObject;
            int count = content.getCount();
            for (int i = 0; i < count; i++) {
                BodyPart part = content.getBodyPart(i);
                if (part.isMimeType("text/plain")) {
                    clearTextPart = part;
                    break;
                } else if (part.isMimeType("text/html")) {
                    htmlTextPart = part;
                }
            }

            if (clearTextPart != null) {
                result = (String) clearTextPart.getContent();
            } else if (htmlTextPart != null) {
                String html = (String) htmlTextPart.getContent();
                result = Jsoup.parse(html).text();
            }

        } else if (contentObject instanceof String) {
            result = (String) contentObject;
        } else {
            System.out.println("notme part or multipart {0}" + message.toString())
            result = null;
        }
        if (result == null) {
            return "Error occurred in parsing content of the message";
        }
        Document doc = Jsoup.parse(result);
        String text = doc.body().text();
        return text;
    }
}

def config = new ConfigSlurper().parse(new File('../conf/process_prod_email_properties.groovy').toURL())
String SSL_FACTORY = "javax.net.ssl.SSLSocketFactory";
Properties props = new Properties();
props.setProperty("mail.pop3.port", config.emailPort + "");
props.setProperty("mail.pop3.ssl.enable", "true");
props.setProperty("mail.pop3s.socketFactory.class", SSL_FACTORY);
props.setProperty("mail.pop3s.socketFactory.fallback", "false");
props.setProperty("mail.pop3s.socketFactory.port", config.emailPort + "");
props.setProperty("mail.pop3.forgettopheaders", "true");

Binding binding = new Binding();
int x = 1
for (a in this.args) {
    println("arg$x: " + a)
    binding.setProperty("arg$x", a);
    x=x+1
}

System.out.println("Connecting to mail server..............");

Session session = Session.getInstance(props, null);
try {
    Store store = session.getStore(config.emailProtocol)
    store.connect(config.emailHost, config.emailPort, binding.getProperty("arg1"), binding.getProperty("arg2"))
    Folder inbox = store.getFolder("INBOX");
    inbox.open(Folder.READ_ONLY);
    FetchProfile fp = new FetchProfile();
    fp.add(UIDFolder.FetchProfileItem.UID);

    System.out.println("Connected successfully............");
    System.out.println("Number of messages fetched : " + inbox.getMessageCount());
    System.out.println("Start :" + new Date());
    File file2 = new File(config.messageIdStoreLocation)
    Map processedMessageMap = loadProcessedMessagesMap(file2)
    Message[] messagesI = inbox.getMessages();
    inbox.fetch(messagesI, fp);
    Address[] ad = InternetAddress.parse(config.recipentsToFilterIn);
    int i = 0;
    for (i = 0; i < messagesI.length; i++) {
        try {
            String msgUId = inbox.getUID(messagesI[i]);

            if (processedMessageMap.get(msgUId) == null) {
                System.out.println("New Message encountered  , UId is " + msgUId);
                Message msg = inbox.getMessage(i + 1);
                def msgId = msg.getHeader("Message-ID")[0];
                System.out.println("Feed needs to be created for this message");
                def issueTitle = msg.getSubject();
                def issueDesc = parseEmailContent(msg);
                //java.net.Authenticator.setDefault(new ProxyAuthenticator(config.proxyUsername, config.proxyPassword));
                Client client = Client.create();
                //client.addFilter(new LoggingFilter(System.out));
                //System.setProperty("http.proxyHost", config.proxyHost);
                //System.setProperty("http.proxyPort", config.proxyPort);

                // DO Stuff
                if (StringUtils.contains(issueTitle,"NEW FEED REGISTRATION")){
                    System.out.println("message id: " + msg.getHeader("Message-ID"));
                    System.out.println("subject is : " + issueTitle);
                    System.out.println(msg.getContent());
                }
                msg.expunged = true;
                processedMessageMap.put(msgUId, "");

            } //end if

        } catch (Exception e) {
            e.printStackTrace()

        }

    }//end for

    saveMessageStoreInDisk(config.messageIdStoreLocation, processedMessageMap);
    System.out.println("Process end time :" + new Date());
}
catch (Exception e) {
    e.printStackTrace();
}