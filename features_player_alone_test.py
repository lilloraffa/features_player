__author__ = "Raffaele Lillo, Valerie Marchenko, Zheng Wang"
__version__ = "1.0"

from pyspark.sql.functions import udf
import datetime

#these define the table/data structure from which the KPI is calculated
tab_pap_player_profile = 'pap.kpi_player_profile'
tab_pap_player_profile_other = 'pap.kpi_player_profile_other'
tab_pap_daily_action = 'pap.daily_action'
tab_pap_daily_action_aggDay = 'daily_action_aggDay'    #Daily aggregation from pap.daily_action
tab_pap_daily_action_pp = 'daily_action_pp'              #Player aggregation from daily_action_aggDay. This is used only when a KPI is not implemented in Player Profile
tab_pap_daily_action_pp_aggDay = 'daily_action_pp_aggDay'     #This will be used to run query on dayAgg table that then is used to build player profile from aggDay table



#db_player_profile_parquet = '/home/admin/data/pap/test/kpi_player_profile_rgs_gsys/'
#db_player_profile_parquet = 'hdfs://LXRP-BIGD-DN-01:8020/user/hive/warehouse/kpi_player_profile/'
#complete data set 
db_player_profile_parquet = '/home/admin/data/pap_v_0_1/player_profile_full'
db_player_profile_other_parquet = '/home/admin/data/pap_v_0_1/player_profile_other_full'
#db_player_profile_parquet = 'hdfs://LXRP-BIGD-DN-01:8020/user/hive/warehouse/pap.db/kpi_player_profile_new'
#db_player_profile_parquet = 'hdfs://LXRP-BIGD-DN-01:8020/user/admin/datalake/pap/dailyaction'
#db_player_profile_parquet = '/user/hive/warehouse/kpi_player_profile_sample/'
#db_player_profile_parquet = '/home/admin/data/pap/test/kpi_player_profile/'
#db_daily_action_parquet = '/home/admin/data/pap/test/daily_action_rgs_gsys/'
#db_daily_action_parquet = 'hdfs://LXRP-BIGD-DN-01:8020/user/admin/datalake/pap/dailyaction_sample'
#complete daily action
#db_daily_action_parquet = 'hdfs://LXRP-BIGD-DN-01:8020/user/admin/dev/datalake/pap/dailyaction'
db_daily_action_parquet = '/home/admin/data/pap_v_0_1/daily_action_full'

#db_daily_action_parquet = '/user/hive/warehouse/daily_action_sample/'
#db_daily_action_parquet = '/home/admin/data/pap/test/daily_action_test/'
#db_games_info_parquet = '/home/admin/data/gap/test/games_info/'
db_games_info_parquet = 'hdfs://LXRP-BIGD-DN-01:8020/user/hive/warehouse/games_info_sample/'
#db_games_info_parquet = '/user/hive/warehouse/games_info_sample/'
tableDict = {
    'player_profile': db_player_profile_parquet,
    'daily_action': db_daily_action_parquet
}
    

#Need to put check to avoid dividing by zero!

'''
tab naming definitions:
- daily_action: daily_action as it is
- daily_action_pp: player profile style KPI calculated from daily_action_aggDay
- daily_action_pp_aggDay: if needed, this is the support kpi to be built in the aggDay that is then used in Player Profile
- daily_action_aggDay: daily aggregation of daily_action table
'''


player_data = {
    ############ Player info ############
    'playerseniority': {
        tab_pap_daily_action: "max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN playerseniority else NULL end) as playerseniority",
        tab_pap_daily_action_aggDay: "max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN playerseniority else NULL end) as playerseniority"
    }, 
    
    'retRatio_hist': {
        tab_pap_daily_action: """count(distinct CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day else NULL end)
        /(max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN playerseniority else NULL end) + 1) as retRatio_hist""",
        tab_pap_daily_action_aggDay: """(sum(count(distinct CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day else NULL end)) over (partition by playerid, customerid, clientid order by day)) / 
        (max((max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN playerseniority ELSE NULL END) + 1)) over (partition by playerid, customerid, clientid order by day)) as retRatio_hist"""
    },
    
    'retRatio_df07': {
        tab_pap_daily_action: "CAST(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)/7.0 as FLOAT) as retRatio_df07",
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day))/7.0 as FLOAT) as retRatio_df07"
    },
    
    'retRatio_df14': {
        tab_pap_daily_action: "CAST(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)/14.0 as FLOAT) as retRatio_df14",
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day))/14.0 as FLOAT) as retRatio_df14"
        
    },
    
    'retRatio_df30': {
        tab_pap_daily_action: "CAST(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)/30.0 as FLOAT) as retRatio_df30",
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day))/30.0 as FLOAT) as retRatio_df30"
        
    },
    
    #for the Player Profile version, the kpi has been developed in the custom list
    'retRatio_d07': {
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW))/7.0 as FLOAT) as retRatio_d07"
    },
    
    #for the Player Profile version, the kpi has been developed in the custom list
    'retRatio_d14': {
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 14 PRECEDING AND CURRENT ROW))/14.0 as FLOAT) as retRatio_d14"
    },
    
    #for the Player Profile version, the kpi has been developed in the custom list
    'retRatio_d30': {
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))/30.0 as FLOAT) as retRatio_d30"
    },
    
    
    
    ############ Historical Plain Measures ############
    'wagConv_hist': {
        tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN coalesce(waghistconv, 0) ELSE 0 END) as wagConv_hist",
        
        tab_pap_daily_action_aggDay: "sum(sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN coalesce(magnvalconv, 0) ELSE 0 END)) over (partition by playerid, customerid, clientid order by day) as wagConv_hist"
    },

    
    'winConv_hist': {
        tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN coalesce(winhistconv, 0) ELSE 0 END) as winConv_hist",
       
        
        tab_pap_daily_action_aggDay: "sum(sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN coalesce(magnwinconv, 0) ELSE 0 END)) over (partition by playerid, customerid, clientid order by day) as winConv_hist"
    },

    
    ############ First days of play (df, wagers) ############
    #All df are now calculated using directly daily actions as it is not properly calculated into player_profile (it is done at game level). Go back to Player Profile once kpi properly calculated
 
    'wagConv_df0': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf0conv ELSE 0 END) as wagConv_df0",
        tab_pap_daily_action: "sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) as wagConv_df0",
        
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as wagConv_df0"
    }, 
    'wagConv_df01': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf1conv ELSE 0 END) as wagConv_df01",
        tab_pap_daily_action: "sum(case when playerseniority<=1 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) as wagConv_df01",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority<=1 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as wagConv_df01"
    },
    'wagConv_df07': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf7conv ELSE 0 END) as wagConv_df07",
        tab_pap_daily_action: "sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) as wagConv_df07",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as wagConv_df07"
    },
    'wagConv_df14': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf14conv ELSE 0 END) as wagConv_df14",
        tab_pap_daily_action: "sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) as wagConv_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as wagConv_df14"
    },
    
    'wagConv_df30': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf30conv ELSE 0 END) as wagConv_df30",
        tab_pap_daily_action: "sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) as wagConv_df30",
        tab_pap_daily_action_aggDay: 'sum(sum(case when playerseniority<=30 then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day) as wagConv_df30'
    },
    
    'wagConv_df0to07': {
        #tab_pap_player_profile: "sum(case when ((wagdf0conv is null) or (wagdf7conv is null) or (wagdf7conv=0)) then 0 else (case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf0conv/wagdf7conv else 0 end) end) as wagConv_df0to07",
        tab_pap_daily_action: """
        case when sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)>0 then 
        (sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)
        / sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) else 0 end as wagConv_df0to07""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day) >0 
        then (
            sum(sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        / sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        ) 
        else 0 end as wagConv_df0to07'''
    },
    
    'wagConv_df07to14': {
        #tab_pap_player_profile: "sum(case when ((wagdf0conv is null) or (wagdf7conv is null) or (wagdf7conv=0)) then 0 else (case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf0conv/wagdf7conv else 0 end) end) as wagConv_df0to07",
        tab_pap_daily_action: """
        case when sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)>0 then 
        (sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)
        / sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) else 0 end as wagConv_df07to14""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day) >0 
        then (
            sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        / sum(sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        ) 
        else 0 end as wagConv_df07to14'''
    },
    
    'wagConv_df07to30': {
        #tab_pap_player_profile: "sum(case when ((wagdf0conv is null) or (wagdf7conv is null) or (wagdf7conv=0)) then 0 else (case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf0conv/wagdf7conv else 0 end) end) as wagConv_df0to07",
        tab_pap_daily_action: """
        case when sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)>0 then 
        (sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)
        / sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) else 0 end as wagConv_df07to30""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day) >0 
        then (
            sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        / sum(sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        ) 
        else 0 end as wagConv_df07to30'''
    },
    
    
    ############ First days of play (df, wins) ############
    'winConv_df0': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf0conv ELSE 0 END) as winConv_df0",
        tab_pap_daily_action: "sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) as winConv_df0",
        
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as winConv_df0"
    }, 
    'winConv_df01': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf1conv ELSE 0 END) as winConv_df01",
        tab_pap_daily_action: "sum(case when playerseniority<=1 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) as winConv_df01",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority<=1 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as winConv_df01"
    },
    'winConv_df07': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf7conv ELSE 0 END) as winConv_df07",
        tab_pap_daily_action: "sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) as winConv_df07",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as winConv_df07"
    },
    'winConv_df14': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf14conv ELSE 0 END) as winConv_df14",
        tab_pap_daily_action: "sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) as winConv_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) as winConv_df14"
    },
    
    'winConv_df30': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN wagdf30conv ELSE 0 END) as winConv_df30",
        tab_pap_daily_action: "sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) as winConv_df30",
        tab_pap_daily_action_aggDay: 'sum(sum(case when playerseniority<=30 then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day) as winConv_df30'
    },
    
    'winConv_df0to07': {
        #tab_pap_player_profile: "sum(case when ((wagdf0conv is null) or (wagdf7conv is null) or (wagdf7conv=0)) then 0 else (case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf0conv/wagdf7conv else 0 end) end) as winConv_df0to07",
        tab_pap_daily_action: """
        case when sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)>0 then 
        (sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)
        / sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) else 0 end as winConv_df0to07""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day) >0 
        then (
            sum(sum(case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        / sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        ) 
        else 0 end as winConv_df0to07'''
    },
    
    'winConv_df07to14': {
        #tab_pap_player_profile: "sum(case when ((wagdf0conv is null) or (wagdf7conv is null) or (wagdf7conv=0)) then 0 else (case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf0conv/wagdf7conv else 0 end) end) as winConv_df0to07",
        tab_pap_daily_action: """
        case when sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)>0 then 
        (sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)
        / sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) else 0 end as winConv_df07to14""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day) >0 
        then (
            sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        / sum(sum(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        ) 
        else 0 end as winConv_df07to14'''
    },
    
    'winConv_df07to30': {
        #tab_pap_player_profile: "sum(case when ((wagdf0conv is null) or (wagdf7conv is null) or (wagdf7conv=0)) then 0 else (case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf0conv/wagdf7conv else 0 end) end) as winConv_df0to07",
        tab_pap_daily_action: """
        case when sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)>0 then 
        (sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)
        / sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) else 0 end as winConv_df07to30""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day) >0 
        then (
            sum(sum(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        / sum(sum(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day) 
        ) 
        else 0 end as winConv_df07to30'''
    },


    ############ Last days of play (d, wagers) ############ 
    'wagConv_d0': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd0conv,0) else 0 end) as wagConv_d0",
        tab_pap_daily_action_aggDay: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) as wagConv_d0"
        
    },
    'wagConv_d01': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 0 else coalesce(wagd1conv) end) as wagConv_d01",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as wagConv_d01"
    },
    'wagConv_d07': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 0 else coalesce(wagd7conv,0) end) as wagConv_d07",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as wagConv_d07"
        #tab_pap_daily_action: 'sum(case when wagd7conv is null then 0 else wagd7conv end) as wagConv_d07'
    },
    
    #KPI player profile is built in the custom section as _d14 is not implemented in the player_profile table
    'wagConv_d14': {
        #tab_pap_player_profile: "sum(case upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 0 else coalesce(wagd7conv,0) end) as wagConv_d07",
        #tab_pap_daily_action:
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as wagConv_d14"
        #tab_pap_daily_action: 'sum(case when wagd7conv is null then 0 else wagd7conv end) as wagConv_d07'
    },
    'wagConv_d30': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv,0) else 0 end) as wagConv_d30",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as wagConv_d30"
    },
    
    'wagConv_d0to07': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv,0) else 0 end)>0 then 
        (sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd0conv,0) else 0 end)
        / sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv,0) else 0 end)) else 0 end as wagConv_d0to07""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) >0 
        then (
            sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end) 
        / sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) 
        ) 
        else 0 end as wagConv_d0to07'''
    },
    
    
    'wagConv_d07to30': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv,0) else 0 end)>0 then 
        (sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv,0) else 0 end)
        / sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv,0) else 0 end)) else 0 end as wagConv_d07to30""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) >0 
        then (
            sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) 
        / sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
        ) 
        else 0 end as wagConv_d07to30'''
    },

    ############ Last days of play (d, wins) ############
        'winConv_d0': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind0conv,0) else 0 end) as winConv_d0",
        tab_pap_daily_action_aggDay: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) as winConv_d0"
        
    },
    'winConv_d01': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 0 else coalesce(wind1conv) end) as winConv_d01",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as winConv_d01"
    },
    'winConv_d07': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 0 else coalesce(wind7conv,0) end) as winConv_d07",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as winConv_d07"
        #tab_pap_daily_action: 'sum(case when wind7conv is null then 0 else wind7conv end) as winConv_d07'
    },
    
    #KPI player profile is built in the custom section as _d14 is not implemented in the player_profile table
    'winConv_d14': {
        #tab_pap_player_profile: "sum(case upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 0 else coalesce(wind7conv,0) end) as winConv_d07",
        #tab_pap_daily_action:
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as winConv_d14"
        #tab_pap_daily_action: 'sum(case when wind7conv is null then 0 else wind7conv end) as winConv_d07'
    },
    'winConv_d30': {
        tab_pap_player_profile: "sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv,0) else 0 end) as winConv_d30",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as winConv_d30"
    },
    
    'winConv_d0to07': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv,0) else 0 end)>0 then 
        (sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind0conv,0) else 0 end)
        / sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv,0) else 0 end)) else 0 end as winConv_d0to07""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) >0 
        then (
            sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end) 
        / sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) 
        ) 
        else 0 end as winConv_d0to07'''
    },
    
    
    'winConv_d07to30': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv,0) else 0 end)>0 then 
        (sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv,0) else 0 end)
        / sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv,0) else 0 end)) else 0 end as winConv_d07to30""",
        
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) >0 
        then (
            sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) 
        / sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
        ) 
        else 0 end as winConv_d07to30'''
    },


    ############ Channel metrics ############
    ## Wagers
    
    'wagConvMob_df07': {
        #tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then wagdf7conv else 0 end) as wagConvMob_df07",
        tab_pap_daily_action: "sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) as wagConvMob_df07",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day) as wagConvMob_df07"
    },
    
    'wagConvMob_df14': {
        tab_pap_daily_action: "sum(case when wherechannel='Mobile' and playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) as wagConvMob_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when wherechannel='Mobile' and playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day) as wagConvMob_df14"
    },
    
    'wagConvMob_df30': {
        #tab_pap_player_profile: "sum(case when channel='Mobile' then wagdf30conv else 0 end) as wagConvMob_df30",
        tab_pap_daily_action: "sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) as wagConvMob_df30",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end) ) over (partition by playerid, customerid, clientid order by day) as wagConvMob_df30"
    },
    
    'wagConvMob_df07to30': {
        tab_pap_daily_action: """
        case when sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end)>0 then
        (sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end)
        / sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end)) ELSE 0 END as wagConvMob_df07to30
         """,
        tab_pap_daily_action_aggDay: """
        case when (sum( sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day))>0 then
        (sum( sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)
        / sum( sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)) ELSE 0 END as wagConvMob_df07to30"""
    },
    
    
    
    'wagConvMob_d07': {
        tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv, 0) else 0 end) as wagConvMob_d07",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as wagConvMob_d07"
    },
    
    'wagConvMob_d30': {
        tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv, 0) else 0 end) as wagConvMob_d30",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as wagConvMob_d30"
    },
    
    'wagConvMob_d07to30': {
        tab_pap_player_profile: """
        case when sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv, 0) else 0 end)>0 then 
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv, 0) else 0 end)
        / sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv, 0) else 0 end)) ELSE 0 END as wagConvMob_d07to30
         """,
        tab_pap_daily_action_aggDay: """
        case when (sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))>0 then 
        ((sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW))
        /(sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))) ELSE 0 END as wagConvMob_d07to30
        """
    },
    
    
    'wagConvMobRatio_d07': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv,0) else 0 end) >0 then 
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv,0) else 0 end)/sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd7conv,0) else 0 end)) else 0 end as wagConvMobRatio_d07
        """,
        tab_pap_daily_action_aggDay: """
        case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) >0 then
        (sum( sum(case when wherechannel='Mobile' then magnvalconv else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)/sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)) else 0 end
        as wagConvMobRatio_d07
        """  
    },
    
    'wagConvMobRatio_d30': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv,0) else 0 end) >0 then 
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv,0) else 0 end)/sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wagd30conv,0) else 0 end)) else 0 end as wagConvMobRatio_d30
        """,
        tab_pap_daily_action_aggDay: """
        case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) >0 then
        (sum( sum(case when wherechannel='Mobile' then magnvalconv else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)/sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)) else 0 end
        as wagConvMobRatio_d07
        """  
    },
    
        'wagConvMob_hist': {
        tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(waghistconv, 0) else 0 end) as wagConvMob_hist",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day) as wagConvMob_hist"
    },
    
    'wagConvMobRatio_hist': {
        tab_pap_player_profile: """
        case when (sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(waghistconv, 0) else 0 end))>0 THEN
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(waghistconv, 0) else 0 end)
        / sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(waghistconv, 0) else 0 end)) ELSE 0 END as wagConvMobRatio_hist
        """,
        tab_pap_daily_action_aggDay: """
        case when (sum( sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day))>0 then 
        (sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)
        /sum( sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)) ELSE 0 END as wagConvMobRatio_hist
        """
    },
    
    ## Winnings
        'winConvMob_df07': {
        #tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then windf7conv else 0 end) as winConvMob_df07",
        tab_pap_daily_action: "sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) as winConvMob_df07",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day) as winConvMob_df07"
    },
    
    'winConvMob_df14': {
        tab_pap_daily_action: "sum(case when wherechannel='Mobile' and playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) as winConvMob_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when wherechannel='Mobile' and playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day) as winConvMob_df14"
    },
    
    'winConvMob_df30': {
        #tab_pap_player_profile: "sum(case when channel='Mobile' then windf30conv else 0 end) as winConvMob_df30",
        tab_pap_daily_action: "sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) as winConvMob_df30",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end) ) over (partition by playerid, customerid, clientid order by day) as winConvMob_df30"
    },
    
    'winConvMob_df07to30': {
        tab_pap_daily_action: """
        case when sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end)>0 then
        (sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end)
        / sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end)) ELSE 0 END as winConvMob_df07to30
         """,
        tab_pap_daily_action_aggDay: """
        case when (sum( sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day))>0 then
        (sum( sum(case when wherechannel='Mobile' and playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)
        / sum( sum(case when wherechannel='Mobile' and playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)) ELSE 0 END as winConvMob_df07to30"""
    },
    
    
    
    'winConvMob_d07': {
        tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv, 0) else 0 end) as winConvMob_d07",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as winConvMob_d07"
    },
    
    'winConvMob_d30': {
        tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv, 0) else 0 end) as winConvMob_d30",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as winConvMob_d30"
    },
    
    'winConvMob_d07to30': {
        tab_pap_player_profile: """
        case when sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv, 0) else 0 end)>0 then 
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv, 0) else 0 end)
        / sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv, 0) else 0 end)) ELSE 0 END as winConvMob_d07to30
         """,
        tab_pap_daily_action_aggDay: """
        case when (sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))>0 then 
        ((sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW))
        /(sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))) ELSE 0 END as winConvMob_d07to30
        """
    },
    
    
  
    
    'winConvMobRatio_d07': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv,0) else 0 end) >0 then 
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv,0) else 0 end)/sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind7conv,0) else 0 end)) else 0 end as winConvMobRatio_d07
        """,
        tab_pap_daily_action_aggDay: """
        case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) >0 then
        (sum( sum(case when wherechannel='Mobile' then magnwinconv else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)/sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)) else 0 end
        as winConvMobRatio_d07
        """  
    },
    
    'winConvMobRatio_d30': {
        tab_pap_player_profile: """
        case when sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv,0) else 0 end) >0 then 
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv,0) else 0 end)/sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(wind30conv,0) else 0 end)) else 0 end as winConvMobRatio_d30
        """,
        tab_pap_daily_action_aggDay: """
        case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv,0) else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) >0 then
        (sum( sum(case when wherechannel='Mobile' then magnwinconv else 0 end) ) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)/sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by playerid, customerid, clientid order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)) else 0 end
        as winConvMobRatio_d07
        """  
    },

    
    'winConvMob_hist': {
        tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(winhistconv, 0) else 0 end) as winConvMob_hist",
        tab_pap_daily_action_aggDay: "sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day) as winConvMob_hist"
    },
    
    'winConvMobRatio_hist': {
        tab_pap_player_profile: """
        case when (sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(winhistconv, 0) else 0 end))>0 THEN
        (sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(winhistconv, 0) else 0 end)
        / sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(winhistconv, 0) else 0 end)) ELSE 0 END as winConvMobRatio_hist
        """,
        tab_pap_daily_action_aggDay: """
        case when (sum( sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day))>0 then 
        (sum( sum(case when wherechannel='Mobile' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)
        /sum( sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnwinconv, 0) else 0 end) ) over (partition by playerid, customerid, clientid order by day)) ELSE 0 END as winConvMobRatio_hist
        """
    },

    
    ############ Intensity of Play (wagers) ############ 
    'wagConv_byActDay_df07': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end) 
        /
        count(distinct case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as wagConv_byActDay_df07
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as wagConv_byActDay_df07
        """
    },
    'wagConv_byActDay_df14': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end) 
        /
        count(distinct case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as wagConv_byActDay_df14
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as wagConv_byActDay_df14
        """
    },
    
    'wagConv_byActDay_df30': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end) 
        /
        count(distinct case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as wagConv_byActDay_df30
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as wagConv_byActDay_df30
        """
    },
    
    
    'wagConvHist_byActDay': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end) 
        /
        count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as wagConvHist_byActDay
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnvalconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as wagConvHist_byActDay
        """
    },
    
    #Commented, as for now I'm using only daily average to have the same kpi for both pp and ts
    '''
    #It seems Gameid and Prodid can be used only in the player profile structure. On TS, there is no window function for count distinct
    
    'wagConvHist_byGameId': {
        tab_pap_player_profile: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then waghistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end), 0) as wagConvHist_byGameId
        """,
        #No window count distinct function, so no TS
        #tab_pap_daily_action_aggDay: """
        
    },
    'wagConvHist_byProd': {
        tab_pap_player_profile: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then waghistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then prod else null end), 0) as wagConvHist_byProd
        """,
        #No window count distinct function, so no TS
        #tab_pap_daily_action_aggDay: """
    },
'''
    
    'avgDayWagConvHist_byGameId': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then waghistconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end), 0) as avgDayWagConvHist_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then waghistconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWagConvHist_byGameId
        """,
        
    },
    
    'avgDayWagConv_df07_byGameId': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then waghistconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then gameid else null end), 0) as avgDayWagConv_df07_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then waghistconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then gameid else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWagConv_df07_byGameId
        """,
        
    },

    'avgDayWagConv_df30_byGameId': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then waghistconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then gameid else null end), 0) as avgDayWagConv_df07_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then waghistconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then gameid else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWagConv_df07_byGameId
        """,
        
    },
    
    'avgDayWagConvHist_byProd': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then waghistconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end), 0) as avgDayWagConvHist_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then waghistconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWagConvHist_byProd
        """,
        
    },
    
    'avgDayWagConv_df07_byProd': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then waghistconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then objprod else null end), 0) as avgDayWagConv_df07_byProd
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then waghistconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then objprod else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWagConv_df07_byProd
        """,
        
    },

    'avgDayWagConv_df30_byProd': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then waghistconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then objprod else null end), 0) as avgDayWagConv_df07_byProd
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then waghistconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then objprod else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWagConv_df07_byProd
        """,
        
    },
    
    

    ############ Intensity of Play (wins) ############
    'winConv_byActDay_df07': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end) 
        /
        count(distinct case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as winConv_byActDay_df07
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when playerseniority<=7 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as winConv_byActDay_df07
        """
    },
    
    'winConv_byActDay_df14': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end) 
        /
        count(distinct case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as winConv_byActDay_df14
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when playerseniority<=14 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as winConv_byActDay_df14
        """
    },
    
    'winConv_byActDay_df30': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end) 
        /
        count(distinct case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as winConv_byActDay_df30
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when playerseniority<=30 and upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as winConv_byActDay_df30
        """
    },
    
    
    'winConvHist_byActDay': {
        
        tab_pap_daily_action:"""
        coalesce(
        sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end) 
        /
        count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)
        , 0) as winConvHist_byActDay
        """,

        tab_pap_daily_action_aggDay: """
        coalesce(
        sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)
        /
        sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else NULL end)) over (partition by customerid, clientid, playerid order by day)
        , 0) as winConvHist_byActDay
        """
    },
    #Commented, as for now I'm using only daily average to have the same kpi for both pp and ts
    '''
    #It seems Gameid and Prodid can be used only in the player profile structure. On TS, there is no window function for count distinct
    
    'winConvHist_byGameId': {
        tab_pap_player_profile: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then winhistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end), 0) as winConvHist_byGameId
        """,
        #No window count distinct function, so no TS
        #tab_pap_daily_action_aggDay: """
        
    },
    'winConvHist_byProd': {
        tab_pap_player_profile: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then winhistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then prod else null end), 0) as winConvHist_byProd
        """,
        #No window count distinct function, so no TS
        #tab_pap_daily_action_aggDay: """
    },
'''
    'avgDayWinConvHist_byGameId': {
        tab_pap_daily_action_pp: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end), 0) as avgDayWinConvHist_byGameId
        """,
        
        
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWinConvHist_byGameId
        """,
        
    },
    
    'avgDayWinConv_df07_byGameId': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then magnwinconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then gameid else null end), 0) as avgDayWinConv_df07_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then gameid else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWinConv_df07_byGameId
        """,
        
    },

    'avgDayWinConv_df30_byGameId': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then magnwinconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then gameid else null end), 0) as avgDayWinConv_df07_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then gameid else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWinConv_df07_byGameId
        """,
        
    },
    
    'avgDayWinConvHist_byProd': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end), 0) as avgDayWinConvHist_byGameId
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWinConvHist_byProd
        """,
        
    },
    
    'avgDayWinConv_df07_byProd': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then magnwinconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then objprod else null end), 0) as avgDayWinConv_df07_byProd
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=7 then objprod else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWinConv_df07_byProd
        """
        
    },

    'avgDayWinConv_df30_byProd': {
        tab_pap_daily_action: """
        coalesce(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then magnwinconv else 0 end)/count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then objprod else null end), 0) as avgDayWinConv_df07_byProd
        """,
        #No window count distinct function, so no TS
        tab_pap_daily_action_aggDay: """
        coalesce(sum(sum(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then magnwinconv else 0 end)) over (partition by customerid, clientid, playerid order by day)/sum(count(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') and playerseniority<=30 then objprod else null end)) over (partition by customerid, clientid, playerid order by day), 0) as avgDayWinConv_df07_byProd
        """
        
    },

    
    ############ Number of Days of Play ############
    'nday_df07': {
        #tab_pap_player_profile: "max(case when wagdf7ndays is null then 0 else wagdf7ndays end) as nday_df07",
        tab_pap_daily_action_pp: "sum(case when nday_df07_pp is not null then nday_df07_pp else 0 end) as nday_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 1 else NULL end) as nday_df07_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day) as nday_df07"
    },
    'nday_df14': {
        #tab_pap_player_profile: "max(case when wagdf7ndays is null then 0 else wagdf7ndays end) as nday_df07",
        tab_pap_daily_action_pp: "sum(case when nday_df14_pp is not null then nday_df14_pp else 0 end) as nday_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 1 else NULL end) as nday_df14_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day) as nday_df14"
    },
    'nday_df30': {
        #tab_pap_player_profile: "max(case when wagdf7ndays is null then 0 else wagdf7ndays end) as nday_df07",
        tab_pap_daily_action_pp: "sum(case when nday_df30_pp is not null then nday_df30_pp else 0 end) as nday_df30",
        tab_pap_daily_action_pp_aggDay: "avg(case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 1 else NULL end) as nday_df30_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day) as nday_df30"
    },
    'nday_hist': {
        #tab_pap_player_profile: "max(case when wagdf7ndays is null then 0 else wagdf7ndays end) as nday_df07",
        tab_pap_daily_action_pp: "sum(case when nday_hist_pp is not null then nday_hist_pp else 0 end) as nday_hist",
        tab_pap_daily_action_pp_aggDay: "avg(case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then 1 else NULL end) as nday_hist_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)) over (partition by playerid, customerid, clientid order by day) as nday_hist"
    },
'''
    'ngame_df0': {
        tab_pap_daily_action_pp: "avg(ngame_df0_pp) as ngame_df0",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df0_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) over (partition by playerid, customerid, clientid order by day) as ngame_df0"
    },
    
    'ngame_df07': {
        tab_pap_daily_action_pp: "avg(ngame_df07_pp) as ngame_df07",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df07_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) over (partition by playerid, customerid, clientid order by day) as ngame_df07"
    },
    
    'ngame_df14': {
        tab_pap_daily_action_pp: "avg(ngame_df14_pp) as ngame_df14",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df14_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) over (partition by playerid, customerid, clientid order by day) as ngame_df14"
    },
    
    'ngame_df30': {
        tab_pap_daily_action_pp: "avg(ngame_df30_pp) as ngame_df30",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df30_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) over (partition by playerid, customerid, clientid order by day) as ngame_df30"
    },

    'ngame_hist': {
        tab_pap_player_profile: "count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then gameid else null end) as ngame_hist",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) over (partition by playerid, customerid, clientid order by day) as ngame_hist"
    },
    
    'ngame_df07to30': {
        tab_pap_daily_action_pp: """
        case when avg(ngame_df30_pp1)>0
        then avg(ngame_df07_pp1)/avg(ngame_df30_pp1)
        else 0 end as ngame_df07to30
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(count(distinct case when playerseniority<=07 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df07_pp1,
        sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df30_pp1
        """,
        tab_pap_daily_action_aggDay: """
        case when sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid)>0 
        then
        (sum(count(distinct case when playerseniority<=07 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid)/
        sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid)) else 0 end as ngame_df07to30
        """
    },
    
    
    'nprod_df0': {
        tab_pap_daily_action_pp: "avg(nprod_df0_pp) as nprod_df0",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) OVER (PARTITION BY playerid, customerid, clientid) as nprod_df0_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority=0 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by playerid, customerid, clientid order by day) as nprod_df0"
    },
    
    'nprod_df07': {
        tab_pap_daily_action_pp: "avg(nprod_df07_pp) as nprod_df07",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) OVER (PARTITION BY playerid, customerid, clientid) as nprod_df07_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=7 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by playerid, customerid, clientid order by day) as nprod_df07"
    },
    
    'nprod_df14': {
        tab_pap_daily_action_pp: "avg(nprod_df14_pp) as nprod_df14",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) OVER (PARTITION BY playerid, customerid, clientid) as nprod_df14_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=14 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by playerid, customerid, clientid order by day) as nprod_df14"
    },
    
    'nprod_df30': {
        tab_pap_daily_action_pp: "avg(nprod_df30_pp) as nprod_df30",
        tab_pap_daily_action_pp_aggDay: "sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) OVER (PARTITION BY playerid, customerid, clientid) as nprod_df30_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by playerid, customerid, clientid order by day) as nprod_df30"
    },
    
    'nprod_hist': {
        tab_pap_player_profile: "count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then prod else null end) as nprod_hist",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objprod else null end)) over (partition by playerid, customerid, clientid order by day) as nprod_hist"
    },
    
    'ngame_df07to30': {
        tab_pap_daily_action_pp: """
        case when avg(ngame_df30_pp1)>0
        then avg(ngame_df07_pp1)/avg(ngame_df30_pp1)
        else 0 end as ngame_df07to30
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(count(distinct case when playerseniority<=07 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df07_pp1,
        sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid) as ngame_df30_pp1
        """,
        tab_pap_daily_action_aggDay: """
        case when sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid)>0 
        then
        (sum(count(distinct case when playerseniority<=07 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid)/
        sum(count(distinct case when playerseniority<=30 AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then objgameid else null end)) OVER (PARTITION BY playerid, customerid, clientid)) else 0 end as ngame_df07to30
        """
    },
'''


    ############ Frequency of Play ############ #Could expand this to have include first two weeks specific metrics
    'd2Play_df07': {
        tab_pap_daily_action_pp: "avg(d2Play_df07) as d2Play_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerseniority+1)<=7 then playerd2play else NULL end) as d2Play_df07",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerseniority+1)<=7 then playerd2play else NULL end)) over (partition by customerid, clientid, playerid order by day) as d2Play_df07"
    },
    'd2PlayGame_df07': {
        tab_pap_daily_action_pp: "avg(d2PlayGame_df07) as d2PlayGame_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerseniority+1)<=7 then playerd2playgame else NULL end) as d2PlayGame_df07",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerseniority+1)<=7 then playerd2playgame else NULL end)) over (partition by customerid, clientid, playerid order by day) as d2PlayGame_df07"
    },
    'd2PlayGameType_df07': {
        tab_pap_daily_action_pp: "avg(d2PlayGameType_df07) as d2PlayGameType_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerseniority+1)<=7 then playerd2playgametype else NULL end) as d2PlayGameType_df07",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerseniority+1)<=7 then playerd2playgametype else NULL end)) over (partition by customerid, clientid, playerid order by day) as d2PlayGameType_df07"
    },
    'd2Play_df14': {
        tab_pap_daily_action_pp: "avg(d2Play_df14) as d2Play_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerseniority+1)<=14 then playerd2play else NULL end) as d2Play_df14",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerseniority+1)<=14 then playerd2play else NULL end)) over (partition by customerid, clientid, playerid order by day) as d2Play_df14"
    },
    'd2PlayGame_df14': {
        tab_pap_daily_action_pp: "avg(d2PlayGame_df14) as d2PlayGame_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerseniority+1)<=14 then playerd2playgame else NULL end) as d2PlayGame_df14",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerseniority+1)<=14 then playerd2playgame else NULL end)) over (partition by customerid, clientid, playerid order by day) as d2PlayGame_df14"
    },
    'd2PlayGameType_df14': {
        tab_pap_daily_action_pp: "avg(d2PlayGameType_df14) as d2PlayGameType_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerseniority+1)<=14 then playerd2playgametype else NULL end) as d2PlayGameType_df14",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerseniority+1)<=14 then playerd2playgametype else NULL end)) over (partition by customerid, clientid, playerid order by day) as d2PlayGameType_df14"
    },
    
    
    
    'd2Play_hist': {
        tab_pap_daily_action_pp: "avg(playerd2play) as d2Play_hist",
        tab_pap_daily_action_pp_aggDay: "avg(playerd2play) as playerd2play",
        tab_pap_daily_action_aggDay: "avg(avg(playerd2play)) over (partition by customerid, clientid, playerid order by day) as d2Play_hist"
    },
    'd2PlayGame_hist': {
        tab_pap_daily_action_pp: "avg(playerd2playgame) as d2PlayGame_hist",
        tab_pap_daily_action_pp_aggDay: "avg(playerd2playgame) as playerd2playgame",
        tab_pap_daily_action_aggDay: "avg(avg(playerd2playgame)) over (partition by customerid, clientid, playerid order by day) as d2PlayGame_hist"
    },
    'd2PlayGameType_hist': {
        tab_pap_daily_action_pp: "avg(playerd2playgametype) as d2PlayGameType_hist",
        tab_pap_daily_action_pp_aggDay: "avg(playerd2playgametype) as playerd2playgametype",
        tab_pap_daily_action_aggDay: "avg(avg(playerd2playgametype)) over (partition by customerid, clientid, playerid order by day) as d2PlayGameType_hist"
    }
}

player_data_custom = {
    'churned': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN waghistconv ELSE 0 END) as wagConv_hist",
        tab_pap_daily_action: """
        CASE WHEN datediff('_PARAM_CHURN_MAXDATE_', max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day ELSE NULL END))> 0
        THEN 1 ELSE 0 END as churned
        """,
        tab_pap_daily_action_aggDay: """
        CASE WHEN datediff('_PARAM_CHURN_MAXDATE_', max(max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day ELSE NULL END)) 
            over (partition by playerid, customerid, clientid))> 0
        THEN 1 ELSE 0 END as churned
        """
    },
    
    'maxdate': {
        #tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN waghistconv ELSE 0 END) as wagConv_hist",
        tab_pap_daily_action: """max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day ELSE NULL END) as maxdate,
        datediff('_PARAM_CHURN_MAXDATE_', max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day ELSE NULL END)) as maxDiff
        """,
        tab_pap_daily_action_aggDay: """max(max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day ELSE NULL END)) 
            over (partition by playerid, customerid, clientid) as maxdate,
        datediff('_PARAM_CHURN_MAXDATE_', max(max(CASE WHEN upper(type) IN ('SESSION', 'WAGER', 'WINNING') THEN day ELSE NULL END)) 
            over (partition by playerid, customerid, clientid)) as maxDiff
        """
        
    },
    
    'ggr_to_today': {
        tab_pap_daily_action_aggDay: """
        sum(sum(magnvalconv - magnwinconv)) over
        (partition by playerid, clientid order by day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ggr_to_today
        """
    },
    
    'ggr_after_today': {
        tab_pap_daily_action_aggDay: """
        sum(sum(magnvalconv - magnwinconv)) over
        (partition by playerid, clientid order by day ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as ggr_after_today
        """
    },
    
    #Only Player Profile Version needs parameter. The TS version is in the usual kpi list
    'retRatio_d07': {
        tab_pap_daily_action: "CAST(count(distinct case when day>date_add('_PARAM_LAST_DAY',-7) AND day<='_PARAM_LAST_DAY' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)/7.0 as FLOAT) as retRatio_d07"
    },
    
    'retRatio_d14': {
        tab_pap_daily_action: "CAST(count(distinct case when day>date_add('_PARAM_LAST_DAY',-14) AND day<='_PARAM_LAST_DAY' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)/14.0 as FLOAT) as retRatio_d14"
    },
    
    'retRatio_d30': {
        tab_pap_daily_action: "CAST(count(distinct case when day>date_add('_PARAM_LAST_DAY',-30) AND day<='_PARAM_LAST_DAY' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then day else null end)/30.0 as FLOAT) as retRatio_d30"
    },
    
    'wagConv_d14': {
        tab_pap_daily_action: "sum(case when day>date_add('_PARAM_LAST_DAY',-14) AND day<='_PARAM_LAST_DAY' AND upper(type) IN ('SESSION', 'WAGER', 'WINNING') then coalesce(magnvalconv) else 0 end) as wagConv_d14"
    }
    
}






    
def getDF_playerProf(sqlContext, where_pp=None, where_da=None, where_inner_da=None, activeRange=['-1', '-1', '-1'], features=[], featuresExlude=[], kpi_custom_param={}, nDailyActions="1000000"):
    global player_data_custom
    
    db_player_profile_list = []
    db_player_profile_other_list = []
    db_daily_action_list = []
    db_daily_action_aggDay_list = []
    db_daily_action_aggDay_list_kpi = []
    
    select_player_profile = ''
    select_player_profile_other = ''
    select_daily_action = ''
    select_daily_action_aggDay = ''
    

    #Merge the two dictionaries so to have the final list of kpi to calculate
    if(len(kpi_custom_param)>0):
        player_data_custom = data_custom_handling(player_data_custom, kpi_custom_param)
        kpi_list = dict(player_data.items() + player_data_custom.items())
    else :
        kpi_list = player_data
    
    for kpi, dic in kpi_list.iteritems():
        if(len(features)==0) | ((len(features)!=0) & (kpi in features) & (kpi not in featuresExlude)): 
            if(tab_pap_player_profile in dic):
                db_player_profile_list.append(dic[tab_pap_player_profile])
            
            #see if kpi from player_profile_other are present
            elif(tab_pap_player_profile_other in dic):
                db_player_profile_other_list.append(dic[tab_pap_player_profile_other])
            
            #see if kpi from simple daily_action are present
            elif(tab_pap_daily_action in dic):
                #print(dic[tab_pap_daily_action])
                db_daily_action_list.append(dic[tab_pap_daily_action])
                
            #see if kpi from aggDaily are present
            elif(tab_pap_daily_action_pp in dic):
                db_daily_action_aggDay_list.append(dic[tab_pap_daily_action_pp])
                if(tab_pap_daily_action_pp_aggDay in dic):
                    db_daily_action_aggDay_list_kpi.append(kpi)


                    
    
    if(len(db_player_profile_list)>0):
        #HERE
        dataPlayerProfile = sqlContext.read.parquet(db_player_profile_parquet)
        dataPlayerProfile.registerTempTable('player_profile')
        
        select_player_profile = ("select playerid, customerid, clientid, " + ', '.join(db_player_profile_list) + ' from player_profile ' +
                                 'where 1=1 ' + (' and (' + where_pp +') ' if where_pp is not None else '') +
                                 'group by playerid, customerid, clientid'
                                 )
        print(select_player_profile)
        
    if(len(db_player_profile_other_list)>0):
        #HERE
        dataPlayerProfile = sqlContext.read.parquet(db_player_profile_other_parquet)
        dataPlayerProfile.registerTempTable('player_profile_other')
        select_player_profile_other = ("select playerid, customerid, clientid, " + ', '.join(db_player_profile_list) + ' from player_profile_other ' +
                                 'where 1=1 ' + (' and (' + where_pp +') ' if where_pp is not None else '') +
                                 'group by playerid, customerid, clientid'
                                 )


    if(len(db_daily_action_list)>0):
        #Need to take care of the day  in where_da
        dataDailyAction = sqlContext.read.parquet(db_daily_action_parquet)
        dataDailyAction.registerTempTable('daily_action')
        
        activePlayer=''
        whereActivePlayer=''
        if (activeRange[0]!='-1' or activeRange[1]!='-1'):
            col = "(max(day) over (partition by playerid, customerid, clientid))"
            
            if activeRange[0]!='-1':
                activePlayer = col + ">='" + str(activeRange[0]) + "'"
            if activeRange[1]!='-1':
                if activePlayer!='':
                    activePlayer = activePlayer + ' and '
                activePlayer = activePlayer + col + "<='" + str(activeRange[1]) + "'"
            activePlayer = "(case when " + activePlayer + " then 1 else 0 end) as check_active"
            if activeRange[2]=='0':
                whereActivePlayer= 'check_active=0'
            else:
                whereActivePlayer= 'check_active=1'
        #print('here: ' + ', '.join(db_daily_action_list)  )
        select_daily_action = ("select playerid, customerid, clientid, " + ', '.join(db_daily_action_list) 
                               + ' from ( '
                               + ' select * '
                               + ((', ' + activePlayer) if activePlayer!='' else '')
                               
                               + " from daily_action "
                               + 'where 1=1 ' + (' and (' + where_inner_da +') ' if where_inner_da is not None else '')
                               + ' ) a '
                               
                               + 'where 1=1 ' + (' and (' + where_da + ') ' if where_da is not None else '')
                               + (' and (' + whereActivePlayer + ') ' if whereActivePlayer !='' else '')
                               
                               
                               #+ ('limit ' + nDailyActions if int(nDailyActions)>0 else '') + ' ' +
                                 #(where_da +' ' if where_da is not None else '') +
                               #'where 1=1 ' + (' and (' + where_da +') ' if where_da is not None else '') +
                               
                               + 'group by playerid, customerid, clientid'

                                 )
        print(select_daily_action)
    
    
    if(len(db_daily_action_aggDay_list)>0):
        #Need to take care of the day  in where_da
        dataDailyAction = getDF_playerTS(sqlContext, ts_period='d', where_da=where_da, activeRange=activeRange, where_inner_da=where_inner_da, features=db_daily_action_aggDay_list_kpi,featuresExlude=featuresExlude, kpi_custom_param=kpi_custom_param, nDailyActions=nDailyActions, isSubQuery=True)
        dataDailyAction.registerTempTable('daily_action_aggDay')
        
        select_daily_action_aggDay = ("select playerid, customerid, clientid, " + ', '.join(db_daily_action_aggDay_list) 
                               + ' from daily_action_aggDay '
                               #+ 'where 1=1 ' + (' and (' + where_da +') ' if where_da is not None else '')
                               #+ ('limit ' + nDailyActions if int(nDailyActions)>0 else '') + ' ' +
                                 #(where_da +' ' if where_da is not None else '') +
                               #'where 1=1 ' + (' and (' + where_da +') ' if where_da is not None else '') +
                               
                                 + 'group by playerid, customerid, clientid'

                                 )
        print(select_daily_action)

    
    
    df_to_join = [];
    if(select_player_profile!=''):
        df_to_join.append(sqlContext.sql(select_player_profile))
    if(select_player_profile_other!=''):
        df_to_join.append(sqlContext.sql(select_player_profile_other))
    if(select_daily_action!=''):
        df_to_join.append(sqlContext.sql(select_daily_action))
    if(select_daily_action_aggDay!=''):
        df_to_join.append(sqlContext.sql(select_daily_action_aggDay))
    
    df_first = df_to_join[0]
    if(len(df_to_join)>1):
        for dfIn in df_to_join[1:] :
            df_first = (df_first.join(dfIn, ['playerid', 'customerid', 'clientid'], 'inner')
                  .drop(dfIn["playerid"])
                  .drop(dfIn["clientid"])
                  .drop(dfIn["customerid"])
                  .select('*'))
    df = df_first
        
    #HERE
    if(select_player_profile!=''):
        sqlContext.dropTempTable('player_profile')
    if(select_player_profile_other!=''):
        sqlContext.dropTempTable('player_profile_other')
    if(select_daily_action!=''):
         sqlContext.dropTempTable('daily_action')
    if(select_daily_action_aggDay!=''):
        sqlContext.dropTempTable('daily_action_aggDay')
    
    

    return df

   


def getDF_playerTS(sqlContext, ts_period='d', where_da=None, where_inner_da=None, activeRange=['-1', '-1', '-1'], features=[], featuresExlude=[], kpi_custom_param={}, nDailyActions="1000000", isSubQuery=False):
    global player_data_custom
    
    #doServiceFeat will create "service features" in case this is used as input table for player profile
    serviceFeat = '''
        sum(magnvalconv) as magnvalconv, 
        sum(magnwinconv) as magnwinconv, 
        max(playerseniority) as playerseniority
    '''
    
    db_daily_action_list = []
    select_daily_action = ''
    
    #Merge the two dictionaries so to have the final list of kpi to calculate
    if(len(kpi_custom_param)>0):
        player_data_custom = data_custom_handling(player_data_custom, kpi_custom_param)
        kpi_list = dict(player_data.items() + player_data_custom.items())
    else :
        kpi_list = player_data
        
    
    for kpi, dic in kpi_list.iteritems():
        if((len(features)==0) & (not isSubQuery)) | ((len(features)!=0) & (kpi in features) & (kpi not in featuresExlude)): 
            
            #define the Hierarchy for which table should be used when multiple definition of kpis are used
            if(isSubQuery):
                if(tab_pap_daily_action_pp_aggDay in dic):
                    db_daily_action_list.append(dic[tab_pap_daily_action_pp_aggDay])
                elif(tab_pap_daily_action_aggDay in dic):
                    db_daily_action_list.append(dic[tab_pap_daily_action_aggDay])
            else:
                if(tab_pap_daily_action_aggDay in dic):
                    db_daily_action_list.append(dic[tab_pap_daily_action_aggDay])
                #elif(tab_pap_daily_action in dic):
                #    db_daily_action_list.append(dic[tab_pap_daily_action])
            

    
    if(len(db_daily_action_list)>0 or isSubQuery):
        #HERE
        dataDailyAction = sqlContext.read.parquet(db_daily_action_parquet)
        dataDailyAction.registerTempTable('daily_action')
        
        activePlayer=''
        whereActivePlayer=''
        if (activeRange[0]!='-1' or activeRange[1]!='-1'):
            #col = "(max(day) over (partition by playerid, customerid, clientid))"
            col = "day "
            
            if activeRange[0]!='-1':
                activePlayer = col + ">='" + str(activeRange[0]) + "'"
            if activeRange[1]!='-1':
                if activePlayer!='':
                    activePlayer = activePlayer + ' and '
                activePlayer = activePlayer + col + "<='" + str(activeRange[1]) + "'"
            #activePlayer = "(case when " + activePlayer + " then 1 else 0 end)as check_active"
            activePlayer = "max(case when " + activePlayer + " then 1 else 0 end) over (partition by customerid, clientid, playerid) as check_active"
            #activePlayerAgg = "max(check_active) over (partition by customerid, clientid, playerid) as check_active"
            if activeRange[2]=='0':
                whereActivePlayer= 'check_active=0'
                #whereActivePlayer='max(check_active) over (partition by customerid, clientid, playerid)=0'
            else:
                whereActivePlayer= 'check_active=1'
                #whereActivePlayer='max(check_active) over (partition by customerid, clientid, playerid)=1'
            #where_check_active = ' check_active=1 '
            
        print()
        
        select_daily_action = ("select * from ( " +
                                "select playerid, customerid, clientid, day, check_active "
                               #+ ((', sum(check_active) as check_active ') if activePlayer else '')
                               + ((', ' + serviceFeat) if isSubQuery else '')
                               + ((', ' + ', '.join(db_daily_action_list)) if len(db_daily_action_list)>0 else '')
                               + ' from ( '
                               + ' select * '
                               + ((', ' + activePlayer) if activePlayer!='' else '')
                               
                               + " from daily_action "
                               + 'where 1=1 ' + (' and (' + where_inner_da +') ' if where_inner_da is not None else '')
                               + ' ) a '
                               + (' where ( ' + whereActivePlayer + ' ) ' if whereActivePlayer!='' else '')
                               + 'group by playerid, customerid, clientid, day, playerseniority, check_active '
                           
                               + ' ) b '
                               
                               + 'where 1=1 ' + (' and (' + where_da +') ' if where_da is not None and not isSubQuery else '')
                               
                               + ('limit ' + nDailyActions if int(nDailyActions)>0 else ''))
        
        
        select_daily_action = ("select * from ( " +
                                "select playerid, customerid, clientid, day "
                               #+ ((', ' + activePlayerAgg) if activePlayer!='' else '')
                               + ((', ' + serviceFeat) if isSubQuery else '')
                               + ((', ' + ', '.join(db_daily_action_list)) if len(db_daily_action_list)>0 else '')
                               + ' from ( '
                               + ' select * '
                               + ((', ' + activePlayer) if activePlayer!='' else '')
                               
                               + " from daily_action "
                               + 'where 1=1 ' + (' and (' + where_inner_da +') ' if where_inner_da is not None else '')
                               + ' ) a '
                               + (' where ( ' + whereActivePlayer + ' ) ' if whereActivePlayer!='' else '')
                               + 'group by playerid, customerid, clientid, day '
                           
                               + ' ) b '
                               
                               + 'where 1=1 ' + (' and (' + where_da +') ' if where_da is not None and not isSubQuery else '')
                               #+ (' and ( ' + whereActivePlayer + ' ) ' if whereActivePlayer!='' else '')
                               + 'order by playerid, customerid, clientid, day'
                               + ('limit ' + nDailyActions if int(nDailyActions)>0 else ''))
        
        
        
        
       

    print(select_daily_action)

    
        
    if(select_daily_action!=''):
        df = sqlContext.sql(select_daily_action)

        print('-----#####----###')
    
    sqlContext.dropTempTable('daily_action')
    return df

#Handle the custom KPI by replacing the variables
def data_custom_handling(kpi_custom, kpi_custom_param):
    custom_kpi_list=[]
    
    #get the list of custom kpi to be processed
    if('list_kpi' in kpi_custom_param):
        custom_kpi_list=kpi_custom_param['list_kpi']
    else:
        custom_kpi_list=kpi_custom.keys()
    
    
    for kpi, kpiDic in kpi_custom.iteritems():
        for kpi_ver, kpi_verVal in kpiDic.iteritems():
            #replace loop
            for replKey, replVal in kpi_custom_param.iteritems():
                if replKey !='list_kpi':
                    kpi_verVal = kpi_verVal.replace(replKey, replVal)
                    kpi_custom[kpi][kpi_ver] = kpi_verVal
    
    return { key:value for key, value in kpi_custom.items() if key in custom_kpi_list }

def getDF_query(sqlContext, query, tableName):
    try:
        dbSelected = tableDict[tableName]
    except:
        print('Selected Table does not exist. Player Profile table used instead.')
        dbSelected = tableDict['player_profile']
    
    print(dbSelected)
    data = sqlContext.read.parquet(dbSelected)
    data.registerTempTable(tableName)
    df = sqlContext.sql(query)
    return df
