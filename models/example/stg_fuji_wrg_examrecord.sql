with
    stage as (
        select
            lnk_patient_visit_exam_modality_site_resource_hk::binary,
            'FUJI' as exam_record_bkcc::varchar,
            patient_hk::binary,
            patient_visit_hk::binary,
            exam_hk::binary,
            modality_hk::binary,
            site_hk::binary,
            resource_hk::binary,
            trim(
                nvl(upper(to_char("VW_CDC_FUJI_WRG_EXAMRECORD"."PATIENTID")), 'NULL')
            ) as patient_id::varchar,
            trim(
                nvl(upper(to_char("VW_CDC_FUJI_WRG_EXAMRECORD"."VISITNUMBER")), 'NULL')
            ) as patient_visit_id::varchar,
            trim(
                nvl(upper(to_char("VW_CDC_FUJI_WRG_EXAMRECORD"."EXAM")), 'NULL')
            ) as exam_id::varchar,
            trim(
                nvl(upper(to_char("VW_CDC_FUJI_WRG_EXAMRECORD"."MODALITY")), 'NULL')
            ) as modality_id::varchar,
            trim(
                nvl(upper(to_char("VW_CDC_FUJI_WRG_EXAMRECORD"."SITECODE")), 'NULL')
            ) as site_id::varchar,
            trim(
                nvl(upper(to_char("VW_CDC_FUJI_WRG_EXAMRECORD"."RESOURCEID")), 'NULL')
            ) as resource_id::varchar, as accessionnumber::number(38, 0),
            as dv_hash_diff::binary,
            'WRG' as dv_tenant_id::varchar,
            'FUJI_WRG.EXAMRECORD' as dv_recsrc::varchar,
            cast(current_timestamp as timestamp) as dv_load_dts::timestamp, as exam
            ::varchar, as completedby::varchar, as technologist::number(
                38,
                0
            ), as patientid::varchar, as resourceid::varchar, as radiologist::number(
                38, 0
            ), as transcriptionist::varchar, as modality::varchar, as status::varchar,
            as archive::varchar, as comments::varchar, as resident::number(
                38, 0
            ), as visitnumber::number(38, 0), as signedby::varchar, as signdate
            ::timestamp_ntz(9), as sitecode::varchar, as schendtime::timestamp_ntz(
                9
            ), as faxed::varchar, as abn::varchar, as precert::varchar, as precertnum
            ::varchar, as mamcat::varchar, as addimaging::varchar, as oneyrfollowup
            ::timestamp_ntz(9), as sixmonfollowup::timestamp_ntz(9), as surgeonname
            ::varchar, as biopsydate::timestamp_ntz(9), as biopsyresults::varchar,
            as followupcomments::varchar, as accuracy::varchar, as printrecall::boolean,
            as printsurgcase::boolean, as printresults::boolean, as mamclose::varchar,
            as confirmed::boolean, as arrivedby::varchar, as scheduledby::number(
                38, 0
            ), as addtocpt::varchar, as recommendations::varchar, as callreport
            ::varchar, as stat::varchar, as overread::varchar, as printed::varchar,
            as printeddate::timestamp_ntz(9), as faxeddate::timestamp_ntz(
                9
            ), as optionalfax::varchar, as faxondemand::varchar, as faxedby::number(
                38, 0
            ), as printedby::number(38, 0), as hold::varchar, as holdnote::varchar,
            as assignedrad::varchar, as readinprogress::varchar, as inprogress::varchar,
            as selfpaycharge::number(38, 5), as qatype::varchar, as qanote::varchar,
            as cancellationreason::number(38, 0), as precertnum2::varchar,
            as precertnum3::varchar, as referralnumberprimarycarrier::varchar,
            as referralnumbersecondarycarrier::varchar, as referralnumbertertiarycarrier
            ::varchar, as precertnumberprimarycarrier::varchar,
            as precertnumbersecondarycarrier::varchar, as precertnumbertertiarycarrier
            ::varchar, as lmp::timestamp_ntz(9), as pregnancyindicator::varchar,
            as onsetofsimilarsymptoms::timestamp_ntz(9), as onsetofcurrentsymptoms
            ::timestamp_ntz(9), as priorstext::varchar, as scheduledon::timestamp_ntz(
                9
            ), as mamletterprintdate::timestamp_ntz(9), as radiologistcode::varchar,
            as chargecode::varchar, as lastprinteddate::timestamp_ntz(
                9
            ), as registrationendedon::timestamp_ntz(9), as registrationendedby::number(
                38, 0
            ), as holddate::timestamp_ntz(9), as heldby::number(38, 0),
            as commentsandhistoryby::number(38, 0), as commentsandhistorylogintype
            ::number(38, 0), as commentsandhistorydate::timestamp_ntz(
                9
            ), as secondaryorderid::varchar, as coded::varchar, as unitprimcharge
            ::number(38, 0), as patientdose::varchar, as addlchargeupdatecnt::number(
                38, 0
            ), as sendonce::varchar, as referrerrecallprinted::boolean, as mamclosedby
            ::number(38, 0), as mamclosedon::timestamp_ntz(
                9
            ), as referrerreviewinprogress::varchar, as dmf::varchar, as notriggerevent
            ::number(38, 0), as ctlungclosed::boolean, as ctlungclosedby::number(
                38,
                0
            ), as ctlungclosedon::timestamp_ntz(9), as scheduledbylogintypeid::number(
                38,
                0
            ), as kiosksessionid::number(38, 0), as precerteffstartdate::timestamp_ntz(
                9
            ), as precerteffenddate::timestamp_ntz(9), as precert2effstartdate
            ::timestamp_ntz(9), as precert2effenddate::timestamp_ntz(
                9
            ), as precert3effstartdate::timestamp_ntz(9), as precert3effenddate
            ::timestamp_ntz(9), as scheduledatetime::timestamp_ntz(
                9
            ), as completiondatetime::timestamp_ntz(9), as orderdatetime::timestamp_ntz(
                9
            ), as begindatetime::timestamp_ntz(9), as arriveddatetime::timestamp_ntz(9),
            as dictateddatetime::timestamp_ntz(9), as provisionaldatetime
            ::timestamp_ntz(9), as finaldatetime::timestamp_ntz(
                9
            ), as addendumdictateddatetime::timestamp_ntz(9), as transcribeddatetime
            ::timestamp_ntz(9), as el_operation::varchar(15), as el_operationtimestamp
            ::timestamp_ntz(9)
        from
            {{ source("STAGE", "VW_CDC_FUJI_WRG_EXAMRECORD") }} "VW_CDC_FUJI_WRG_EXAMRECORD"
    )

select *
from stage
