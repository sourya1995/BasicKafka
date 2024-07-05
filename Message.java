import java.io.Serializable;
import java.util.Optional;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private String content;
    private int partition;

    public Message(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "Message [content=" + content + ", partition=" + partition + "]";
    }

    

   
}
