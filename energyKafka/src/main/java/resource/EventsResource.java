package resource;

import com.example.BatteryEvent;
import com.example.kafka.KafkaEventProducer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Path("/")
public class EventsResource {

    private static final String KAFKA_EVENT_TOPIC = "battery_event";

    private KafkaEventProducer<String, BatteryEvent> kafkaProducer = KafkaEventProducer.newInstance(KAFKA_EVENT_TOPIC);


    @Path("{id}")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response newEvent(@PathParam("id") String id, String batteryEvents) throws IOException {
        System.out.println("id:" + id);

        batteryEvents = "[" + batteryEvents + "]";
        batteryEvents = batteryEvents.replaceAll("\\}\\R\\{", "},{");

        ObjectMapper mapper = new ObjectMapper();
        List<BatteryEvent> events = mapper.readValue(batteryEvents, new TypeReference<List<BatteryEvent>>(){});

        for(BatteryEvent batteryEvent: events) {
            kafkaProducer.produceRecord(batteryEvent);
        }

        System.out.println("MessageList:" + Arrays.toString(events.toArray()));
        return Response.ok().build();
    }


}
