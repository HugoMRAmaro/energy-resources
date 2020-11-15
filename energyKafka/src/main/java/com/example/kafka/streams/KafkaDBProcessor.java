package com.example.kafka.streams;

import com.example.BatteryEvent;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.jdbi.v3.core.Jdbi;
import repository.EventDao;

public class KafkaDBProcessor implements Processor<String, BatteryEvent> {

    private final Jdbi jdbi;
    private final EventDao eventDao;

    public KafkaDBProcessor(Jdbi jdbi) {
        this.jdbi = jdbi;
        this.eventDao = jdbi.onDemand(EventDao.class);
    }
    
    @Override
    public void init(ProcessorContext processorContext) {

    }


    @Override
    public void process(String s, BatteryEvent batteryEvent) {
        jdbi.useHandle(handle -> {
            handle.begin();
            eventDao.addEvent(batteryEvent.getChargingSource(), batteryEvent.getProcessor4Temp(),
                    batteryEvent.getDeviceId(), batteryEvent.getProcessor2Temp(), batteryEvent.getProcessor1Temp(),
                    batteryEvent.getCharging(), batteryEvent.getCurrentCapacity(), batteryEvent.getInverterState(),
                    batteryEvent.getModuleLTemp(), batteryEvent.getModuleRTemp(), batteryEvent.getProcessor3Temp(),
                    batteryEvent.getSoCRegulator());
            System.out.println("########### EVENT ADDED");
            handle.commit();
            handle.close();

        });

    }

    @Override
    public void close() {

    }
}
