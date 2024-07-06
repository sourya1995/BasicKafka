package Kafka;
import java.io.Serializable;
import java.util.Optional;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private String content;
    private int partition;
    private int id;

    public Message(String content, int id) {
        this.content = content;
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public int getPartition() {
        return partition;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Message [content=" + content + ", partition=" + partition + ", id=" + id + "]";
    }

}
