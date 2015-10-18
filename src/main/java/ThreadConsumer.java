import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ramandika on 18/10/15.
 */
public class ThreadConsumer implements Runnable {
    private String chname;
    private ConsumerConnector connector;

    public ThreadConsumer(String chname,ConsumerConnector connector){
        this.connector=connector;
        this.chname=chname;
    }

    public void run() {
        Map<String,Integer> topicCountMap=new HashMap<String, Integer>();
        topicCountMap.put(chname,1);
        System.out.println(topicCountMap);
        Map<String,List<KafkaStream<byte[],byte[]>>> temp=connector.createMessageStreams(topicCountMap);
        ConsumerIterator<byte[], byte[]> it = temp.get(chname).get(0).iterator();
        while (it.hasNext()) {
            System.out.println(new String(it.next().message()));
        }
        System.out.println("Leave channel " + chname);
    }
}
