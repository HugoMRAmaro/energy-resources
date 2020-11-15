package repository;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;


public interface EventDao {


    @SqlUpdate("insert into device_events( charging_source, processor4_temp, device_id, processor2_temp, processor1_temp,\n" +
            "   charging, current_capacity, inverter_state, moduleL_temp, moduleR_temp, processor3_temp, SoC_regulator)\n" +
            "values ( :charging_source, :processor4_temp, :device_id, :processor2_temp, :processor1_temp,\n" +
            "   :charging, :current_capacity, :inverter_state, :moduleL_temp, :moduleR_temp, :processor3_temp, :SoC_regulator)")
    void addEvent(@Bind("charging_source") String charging_source,
                 @Bind("processor4_temp") Integer processor4_temp,
                 @Bind("device_id") String device_id,
                 @Bind("processor2_temp") Integer processor2_temp,
                 @Bind("processor1_temp") Integer processor1_temp,
                 @Bind("charging") Integer charging,
                 @Bind("current_capacity") Integer current_capacity,
                 @Bind("inverter_state") Integer inverter_state,
                 @Bind("moduleL_temp") Integer moduleL_temp,
                 @Bind("moduleR_temp") Integer moduleR_temp,
                 @Bind("processor3_temp") Integer processor3_temp,
                 @Bind("SoC_regulator") Float SoC_regulator
                 );
}
