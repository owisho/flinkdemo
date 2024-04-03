package per.owisho.learn.window;

import java.io.Serializable;
import java.util.Objects;

public class EventBean implements Serializable {
    private String id;
    private Integer ts;
    public Integer value;

    public EventBean() {
    }

    public EventBean(String id, Integer ts, Integer value) {
        this.id = id;
        this.ts = ts;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getTs() {
        return ts;
    }

    public void setTs(Integer ts) {
        this.ts = ts;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "EventBean{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventBean eventBean = (EventBean) o;
        return Objects.equals(id, eventBean.id) && Objects.equals(ts, eventBean.ts) && Objects.equals(value, eventBean.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, value);
    }
}
