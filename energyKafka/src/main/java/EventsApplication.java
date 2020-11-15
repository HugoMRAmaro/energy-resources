import com.example.kafka.streams.KafkaEventTopology;
import configuration.EventsConfiguration;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.jdbi.v3.core.Jdbi;
import io.dropwizard.jdbi3.JdbiFactory;
import resource.EventsResource;
import resource.JsonInformativeExceptionMapper;

public class EventsApplication extends Application<EventsConfiguration> {


    public static void main(String[] args) throws Exception {
        new EventsApplication().run(args);
    }

    @Override
    public String getName() {
        return "events-application";
    }

    @Override
    public void initialize(Bootstrap<EventsConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(EventsConfiguration configuration,
                    Environment environment) {
        final JdbiFactory factory = new JdbiFactory();
        final Jdbi jdbi = factory.build(environment, configuration.getDataSourceFactory(), "postgresql");

        System.out.println("jbi: " + jdbi.toString());
        final EventsResource eventsResource = new EventsResource();
        environment.jersey().register(eventsResource);
        environment.jersey().register(new JsonInformativeExceptionMapper());

        KafkaEventTopology kafkaEventTopology = new KafkaEventTopology();
        kafkaEventTopology.start(jdbi);
    }

}
