/**
 * Created by ramandika on 16/10/15.
 */

import java.lang.String;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import kafka.Kafka;
import kafka.admin.AdminUtils;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.proto.ExistsRequest;

public class Client{
    private String zookeeper,broker,nickname;
    private ConsumerConnector connector;
    private Map<String,ConsumerConnector> connectors;
    private ConsumerConfig conf;

    private String getConsumerGroup(String nickname){
        return "consumer-group-"+nickname;
    }

    private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }
    //Instantiate object client
    public Client(String zookeeper,String broker){
        this.zookeeper=zookeeper;
        this.broker=broker;
        connectors=new HashMap<>();
    }

    //Create Nick
    public void createNick(String nickname) {
        if(this.nickname!=null) System.out.println("You have given a nickname");
        else {
            this.nickname = nickname;
            System.out.println("Welocome "+this.nickname+" !!!");
        }
    }

    //Join a channel
    public void joinChannel(String chname) {
        boolean timeout=false;
        if(connectors.containsKey(chname)) System.out.println("You have joined "+chname+" channel before");
        else{
            conf=createConsumerConfig(zookeeper,getConsumerGroup(this.nickname));
            connector=Consumer.createJavaConsumerConnector(conf);
            connectors.put(chname,connector);
            try{
                //Create topic first
                createTopic(chname);
                System.out.println("Join " + chname + " success");
                ThreadConsumer t=new ThreadConsumer(chname,connector);
                Thread thread = new Thread(t);
                thread.start();
                timeout=false;
                System.out.println("Created channel " + chname);
                System.out.println("You join "+chname);
            }catch(Exception e){
                if(e.getMessage().contains("already exist") && e.getMessage().contains("Topic"))timeout=true;
            }
            if(timeout){
                ThreadConsumer t=new ThreadConsumer(chname,connector);
                Thread thread = new Thread(t);
                thread.start();
                System.out.println("You join "+chname);
            }
        }
    }

    private KafkaProducer getProducer() {
        java.util.Map<java.lang.String,java.lang.Object> configs = new HashMap<String, Object>();
        configs.put("bootstrap.servers",broker);
        configs.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        configs.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer(configs);
    }

    //Leave a channel
    public void leaveChannel(String chname) {
        ConsumerConnector connector=connectors.get(chname);
        if(connector!=null){
            connector.shutdown();
            connectors.remove(chname);
        }else{
            System.out.println("Not connected to the channel");
        }
    }

    //Terminate program
    public void exit() {
        for (Map.Entry<String, ConsumerConnector> entry : connectors.entrySet())
        {
            entry.getValue().shutdown();
        }
        connectors.clear();
    }


    public Map<String, List<KafkaStream<byte[], byte[]>>> getMultipleChannelStream(List<String> channels){
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();;
        for(String channel:channels){
            topicCountMap.put(channel, 1);
        }
        return connector.createMessageStreams(topicCountMap);
    }

    public List<KafkaStream<byte[],byte[]>> getSingleChannelStream(String channel){
        Map<String,Integer> topicCountMap=new HashMap<String, Integer>();
        topicCountMap.put(channel,1);
        Map<String,List<KafkaStream<byte[],byte[]>>> temp=connector.createMessageStreams(topicCountMap);
        if(temp!=null){
            return temp.get(channel);
        }
        return null;
    }

    public void sendMessage(String chname,String message){
        if(connectors.containsKey(chname)) {
            KafkaProducer producer = getProducer();
            String modifiedMessage = "[" + nickname + "][" + chname + "]: " + message;
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<byte[], byte[]>(chname,
                            modifiedMessage.getBytes());
            producer.send(record);
            producer.close();
        }else{
            System.out.println("You haven't join "+chname+" channel");
        }
    }

    public void sendMessage(String message){
        for (Map.Entry<String, ConsumerConnector> entry : connectors.entrySet())
        {
            sendMessage(entry.getKey(),message);
        }
    }

    //Create topic
    public void createTopic(String chname)throws RuntimeException{
        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        ZkClient zkClient = new ZkClient(zookeeper, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);

        int numPartitions = 1;
        int replicationFactor = 1;
        AdminUtils.createTopic(zkClient, chname, numPartitions, replicationFactor, new Properties());
    }

    public static void main(String[] args){
        //String zooKeeper = args[0];//"localhost:2181";
        //String server= args[1];//"localhost:9092";

        String zooKeeper="localhost:2181";
        String server="localhost:9092";

        Client client=new Client(zooKeeper,server);
        String input,command;
        Scanner scan=new Scanner(System.in);
        input=scan.nextLine();
        while(!input.equals("/EXIT")){
            if(input.contains(" ")) command=input.substring(0,input.indexOf(' '));
            else command=input;
            switch (command){
                case "/NICK":
                    if(input.equals("/NICK")){
                        StringGenerator generator=new StringGenerator(8);
                        client.createNick(generator.nextString());
                    }
                    else{
                        try {
                            input = input.substring(input.indexOf(' ') + 1, input.length());
                            client.createNick(input);
                        }catch (Exception e){
                            System.out.println("Input not valid, please type /NICK [Name] or just /NICK for random nickname");
                        }
                    }
                    break;
                case "/JOIN":
                    try {
                        input = input.substring(input.indexOf(' ') + 1, input.length());
                        client.joinChannel(input);
                    }catch (Exception e){
                        System.out.println("Input not valid, please type /JOIN [Channel_Name]");
                    }
                    break;
                case "/LEAVE":
                    try{
                        input=input.substring(input.indexOf(' ')+1,input.length());
                        client.leaveChannel(input);
                    }catch (Exception e){
                        System.out.println("Input not valid, please type /LEAVE [Channel_Name]");
                    }
                    break;
                default:
                    if(command.length()>0) {
                        try{
                            if(command.charAt(0)=='@'){
                                String chname=input.substring(1,input.indexOf(' '));
                                String message=input.substring(input.indexOf(' ')+1,input.length());
                                client.sendMessage(chname,message);
                            }else{
                                String message=input.substring(0,input.length());
                                client.sendMessage(message);
                            }
                        }catch(Exception e){
                            System.out.println("Input not valid, please type @[channel_name] [message] or [message without @ as first character]");
                        }
                    }
                    break;
            }
            input=scan.nextLine();
        }
        client.exit();
    }
}
