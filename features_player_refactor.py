
from pyspark.sql.functions import udf
import datetime
# these define the table/data structure from which the KPI is calculated
tab_pap_player_profile = 'pap.kpi_player_profile'
tab_pap_player_profile_other = 'pap.kpi_player_profile_other'
tab_pap_daily_action = 'pap.daily_action'
# Daily aggregation from pap.daily_action
tab_pap_daily_action_aggDay = 'daily_action_aggDay'
# Player aggregation from daily_action_aggDay. This is used only when a
# KPI is not implemented in Player Profile
tab_pap_daily_action_pp = 'daily_action_pp'
# This will be used to run query on dayAgg table that then is used to
# build player profile from aggDay table
tab_pap_daily_action_pp_aggDay = 'daily_action_pp_aggDay'

player_data = {
    ############ Player info ############
    'playerseniority': {
        # does not require anything else as playerseniority is used by aggDay
        # by default
        tab_pap_daily_action_pp: 'max(playerSeniority) as playerseniority',
        tab_pap_daily_action_aggDay: 'max(playerSeniority) as playerseniority'
    },
    'retRatio_hist': {
        tab_pap_daily_action_pp: "count(distinct day)/(max(playerSeniority) + 1) as retRatio_hist",
        tab_pap_daily_action_aggDay: '''(sum(count(distinct day)) over (partition by playerid, customerid, clientid order by day)) /
        (max((max(playerseniority) + 1)) over (partition by playerid, customerid, clientid order by day)) as retRatio_hist'''
    },
    'retRatio_df07': {
        tab_pap_daily_action_pp: "CAST(count(distinct case when playerSeniority<7 then day else null end)/7.0 as FLOAT) as retRatio_df07",
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when playerSeniority<7 then day else null end)) over (partition by playerId, customerId, clientId order by day))/7.0 as FLOAT) as retRatio_df07"
    },
    'retRatio_df14': {
        tab_pap_daily_action_pp: "CAST(count(distinct case when playerSeniority<14 then day else null end)/14.0 as FLOAT) as retRatio_df14",
        tab_pap_daily_action_aggDay: "CAST((sum(count(distinct case when playerSeniority<14 then day else null end)) over (partition by playerId, customerId, clientId order by day))/14.0 as FLOAT) as retRatio_df14",
    },


    ############ Historical Plain Measures ############
    'wagConv_hist': {
        tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER') THEN wagHistconv ELSE 0 END) as wagConv_hist",
        tab_pap_daily_action: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER') THEN magnValConv ELSE 0 END) as wagConv_hist",
        tab_pap_daily_action_aggDay: "sum(sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER') THEN magnValConv ELSE 0 END)) over (partition by playerId, customerId, clientId order by day) as wagConv_hist"
    },
    'winConv_hist': {
        tab_pap_player_profile: "sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER') THEN winHistconv ELSE 0 END) as winConv_hist",
        tab_pap_daily_action_aggDay: "sum(sum(CASE WHEN upper(type) IN ('SESSION', 'WAGER') THEN magnWinConv ELSE 0 END)) over (partition by playerId, customerId, clientId order by day) as winConv_hist"
    },
	
    ############ First days of play (df, wagers) ############
    # All df are now calculated using directly daily actions as it is not
    # properly calculated into player_profile (it is done at game level)
    'wagConv_df0': {
        tab_pap_daily_action_pp: "sum(wagConv_df0) as wagConv_df0",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df0",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as wagConv_df0"
    },
    'wagConv_df01': {
        tab_pap_daily_action_pp: "sum(wagConv_df01) as wagConv_df01",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<=1 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df01",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority<=1 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as wagConv_df01"
    },
    'wagConv_df07': {
        tab_pap_daily_action_pp: "sum(wagConv_df07) as wagConv_df07",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df07",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as wagConv_df07"
    },
    'wagConv_df14': {
        tab_pap_daily_action_pp: "sum(wagConv_df14) as wagConv_df14",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as wagConv_df14"
    },
    'wagConv_df30': {
        tab_pap_daily_action_pp: "sum(wagConv_df30) as wagConv_df30",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df30",
        tab_pap_daily_action_aggDay: 'sum(sum(case when (playerSeniority + 1)<30 then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as wagConv_df30'
    },
    'wagConv_df0to07': {
        tab_pap_daily_action_pp: "case when sum(wagConv_df07_pp)>0 then sum(wagConv_df0_pp)/sum(wagConv_df07_pp) else 0 end as wagConv_df0to07",
        tab_pap_daily_action_pp_aggDay: '''
        sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df0_pp,
        sum(case when playerSeniority<=7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df07_pp
        ''',
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0
        then (
            sum(sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        / sum(sum(case when playerSeniority<=7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        )
        else 0 end as wagConv_df0to07'''
    },
    'wagConv_df07to14': {
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0
        then (
            sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        / sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        )
        else 0 end as wagConv_df07to14''',
        tab_pap_daily_action_pp: "case when sum(wagConv_df14_pp1)>0 then sum(wagConv_df07_pp1)/sum(wagConv_df14_pp1) else 0 end as wagConv_df07to14",
        tab_pap_daily_action_pp_aggDay: '''
        sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df07_pp1,
        sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df14_pp1
        ''',
    },
    'wagConv_df07to30': {
        tab_pap_daily_action_pp: "case when sum(wagConv_df30_pp2)>0 then sum(wagConv_df07_pp2)/sum(wagConv_df30_pp2) else 0 end as wagConv_df07to30",
        tab_pap_daily_action_pp_aggDay: '''
        sum(case when playerSeniority<=7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df07_pp2,
        sum(case when playerSeniority<=30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_df30_pp2
        ''',
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerSeniority<=30 then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0
        then (
            sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        / sum(sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        )
        else 0 end as wagConv_df07to30'''
    },

    ############ First days of play (df, wins) ############
    'winConv_df0': {
        tab_pap_daily_action_pp: "sum(winConv_df0) as winConv_df0",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df0",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as winConv_df0"
    },
    'winConv_df01': {
        tab_pap_daily_action_pp: "sum(winConv_df01) as winConv_df01",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<=1 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df01",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority=1 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as winConv_df01"
    },
    'winConv_df07': {
        tab_pap_daily_action_pp: "sum(winConv_df07) as winConv_df07",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df07",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as winConv_df07"
    },
    'winConv_df14': {
        tab_pap_daily_action_pp: "sum(winConv_df14) as winConv_df14",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as winConv_df14"
    },
    'winConv_df30': {
        tab_pap_daily_action_pp: "sum(winConv_df30) as winConv_df30",
        tab_pap_daily_action_pp_aggDay: "sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df30",
        tab_pap_daily_action_aggDay: "sum(sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as winConv_df30"
    },
    'winConv_df0to07': {
        tab_pap_daily_action_pp: "case when sum(winConv_df07_pp)>0 then sum(winConv_df0_pp)/sum(winConv_df07_pp) else 0 end as winConv_df0to07",
        tab_pap_daily_action_pp_aggDay: '''
        sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df0_pp,
        sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df07_pp
        ''',
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0
        then (
            sum(sum(case when playerSeniority=0 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        / sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        )
        else 0 end as winConv_df0to07'''
    },
    'winConv_df07to14': {
        tab_pap_daily_action_pp: "case when sum(winConv_df14_pp1)>0 then sum(winConv_df07_pp1)/sum(winConv_df14_pp1) else 0 end as winConv_df07to14",
        tab_pap_daily_action_pp_aggDay: '''
        sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df07_pp1,
        sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df14_pp1
        ''',
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0
        then (
            sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        / sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        )
        else 0 end as winConv_df07to14''',
    },
    'winConv_df07to30': {

        tab_pap_daily_action_pp: "case when sum(winConv_df30_pp2)>0 then sum(winConv_df07_pp2)/sum(winConv_df30_pp2) else 0 end as winConv_df07to30",
        tab_pap_daily_action_pp_aggDay: '''
        sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df07_pp2,
        sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_df30_pp2
        ''',
        tab_pap_daily_action_aggDay: '''case when sum(sum(case when (playerSeniority + 1)<30 then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0
        then (
            sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        / sum(sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day)
        )
        else 0 end as winConv_df07to30'''
    },


    ############ Last days of play (d, wagers) ############
    'wagConv_d0': {
        tab_pap_player_profile: "sum(case when wagd0Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then wagd0Conv else 0 end) as wagConv_d0",
        tab_pap_daily_action_aggDay: "sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_d0"

    },
    'wagConv_d01': {
        tab_pap_player_profile: "sum(case when wagd1Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then 0 else wagd1Conv end) as wagConv_d01",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as wagConv_d01"
    },
    'wagConv_d07': {
        tab_pap_player_profile: "sum(case when wagd7Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then 0 else wagd7Conv end) as wagConv_d07",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as wagConv_d07"
        # tab_pap_daily_action: 'sum(case when wagd7conv is null then 0 else
        # wagd7conv end) as wagConv_d07'
    },
    'wagConv_d30': {
        tab_pap_player_profile: 'sum(case when wagd30Conv is not NULL then 0 else wagd30Conv end) as wagConv_d30',
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as wagConv_d30"
    },

    ############ Last days of play (d, wins) ############
    'winConv_d0': {
        tab_pap_player_profile: "sum(case when wind0Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then wind0Conv else 0 end) as winConv_d0",
        tab_pap_daily_action_aggDay: "sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_d0"
    },
    'winConv_d01': {
        tab_pap_player_profile: "sum(case when wind1Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then wind1Conv else 0 end) as winConv_d01",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as winConv_d01"
    },
    'winConv_d07': {
        tab_pap_player_profile: "sum(case when wind7Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then wind7Conv else 0 end) as winConv_d07",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as winConv_d07"
    },
    'winConv_d30': {
        tab_pap_player_profile: "sum(case when wind30Conv is not NULL AND upper(type) IN ('SESSION', 'WAGER') then wind30Conv else 0 end) as winConv_d30",
        tab_pap_daily_action_aggDay: "sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as winConv_d30"
    },


    ############ Channel metrics ############
    ###### Wagers #######
    'wagConvMob_df07': {
        tab_pap_daily_action_pp: "sum(wagConvMob_df07) as wagConvMob_df07",
        tab_pap_daily_action_pp_aggDay: "sum(case when whereChannel<>'Internet' and playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConvMob_df07",
        tab_pap_daily_action_aggDay: "sum( sum(case when whereChannel<>'Internet' and playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as wagConvMob_df07"
    },
    'wagConvMob_df14': {
        tab_pap_daily_action_pp: "sum(wagConvMob_df14) as wagConvMob_df14",
        tab_pap_daily_action_pp_aggDay: "sum(case when whereChannel<>'Internet' and playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConvMob_df14",
        tab_pap_daily_action_aggDay: "sum(sum(case when whereChannel<>'Internet' and playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) as wagConvMob_df14"
    },
    'wagConvMob_df30': {
        tab_pap_daily_action_pp: "sum(wagConvMob_df30) as wagConvMob_df30",
        tab_pap_daily_action_pp_aggDay: "sum(case when whereChannel<>'Internet' and playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConvMob_df30",
        tab_pap_daily_action_aggDay: "sum( sum(case when whereChannel<>'Internet' and playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as wagConvMob_df30"
    },
    'wagConvMob_hist': {
        #tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER') then wagHistconv else 0 end) as wagConvMob_hist",
        tab_pap_daily_action_aggDay: "sum( sum(case when whereChannel<>'Internet' AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as wagConvMob_hist"
    },
    'wagConvHistMob_ratio': {
        #tab_pap_player_profile: """
        #case when sum(case when upper(type) IN ('SESSION', 'WAGER') then wagHistconv else 0 end) >0 then
        #(sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER') then wagHistconv else 0 end)/sum(case when upper(type) IN ('SESSION', 'WAGER') then wagHistconv else 0 end)) else 0 end as wagConvHistMob_ratio
        #""",
        tab_pap_daily_action_aggDay: """
        case when sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day) >0 then
        (sum( sum(case when wherechannel<>'Internet' then magnValConv else 0 end) ) over (partition by playerId, customerId, clientId order by day)/sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day)) else 0 end
        as wagConvHistMob_ratio
        """
    },

    #### Winnings #####
    'winConvMob_df07': {
        tab_pap_daily_action_pp: "sum(winConvMob_df07) as winConvMob_df07",
        tab_pap_daily_action_pp_aggDay: "sum(case when whereChannel<>'Internet' and playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConvMob_df07",
        tab_pap_daily_action_aggDay: "sum( sum(case when whereChannel<>'Internet' and playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as winConvMob_df07"

    },
    'winConvMob_df14': {
        tab_pap_daily_action_pp: "sum(winConvMob_df14) as winConvMob_df14",
        tab_pap_daily_action_pp_aggDay: "sum(case when whereChannel<>'Internet' and playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConvMob_df14",
        tab_pap_daily_action_aggDay: "sum( sum(case when whereChannel<>'Internet' and playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as winConvMob_df14"
    },
    'winConvMob_df30': {
        tab_pap_daily_action_pp: "sum(winConvMob_df30) as winConvMob_df30",
        tab_pap_daily_action_pp_aggDay: "sum(case when whereChannel<>'Internet' and playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConvMob_df30",
        tab_pap_daily_action_aggDay: "sum(sum(case when whereChannel<>'Internet' and playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as winConvMob_df30"
    },
    'winConvMob_hist': {
        #tab_pap_player_profile: "sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER') then winHistconv else 0 end) as winConvMob_hist",
        tab_pap_daily_action_aggDay: "sum( sum(case when whereChannel<>'Internet' AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) ) over (partition by playerId, customerId, clientId order by day) as winConvMob_hist"
    },
    'winConvHistMob_ratio': {
        #tab_pap_player_profile: """
        #case when sum(case when upper(type) IN ('SESSION', 'WAGER') then winHistconv else 0 end) >0 then
        #(sum(case when channel='Mobile' AND upper(type) IN ('SESSION', 'WAGER') then winHistconv else 0 end)/sum(case when upper(type) IN ('SESSION', 'WAGER') then winHistconv else 0 end)) else 0 end as winConvHistMob_ratio
        #""",
        tab_pap_daily_action_aggDay: "sum(sum(case when whereChannel<>'Internet' AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) ) over (partition by playerId, customerId, clientId order by day)/sum(magnWinConv) as winConvHistMob_ratio"
    },


    ############ Intensity of Play (wagers) ############
    'wagConv_byActDay_df07': {
        tab_pap_daily_action_pp: """
        case when sum(wagConv_byActDay_df07__nDay_df7)>0
        then
        (sum(wagConv_byActDay_df07__wagConv_df7)/sum(case when wagConv_byActDay_df07__nDay_df7 is not null then wagConv_byActDay_df07__nDay_df7 else 0 end))
        else 0 end as wagConv_byActDay_df07
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_byActDay_df07__wagConv_df7,
        avg(case when playerSeniority<7 then (case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) else NULL end) as wagConv_byActDay_df07__nDay_df7
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as wagConv_byActDay_df07
        """
    },
    'wagConv_byActDay_df14': {
        tab_pap_daily_action_pp: """
        case when sum(wagConv_byActDay_df14__nDay_df14)>0
        then
        (sum(wagConv_byActDay_df14__wagConv_df14)/sum(case when wagConv_byActDay_df14__nDay_df14 is not null then wagConv_byActDay_df14__nDay_df14 else 0 end))
        else 0 end as wagConv_byActDay_df14
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_byActDay_df14__wagConv_df14,
        avg(case when playerSeniority<14 then (case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) else NULL end) as wagConv_byActDay_df14__nDay_df14
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when playerSeniority<=14 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when playerSeniority<=14 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when playerSeniority<=14 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as wagConv_byActDay_df14
        """
    },
    'wagConv_byActDay_df30': {
        tab_pap_daily_action_pp: """
        case when sum(wagConv_byActDay_df30__nDay_df30)>0
        then
        (sum(wagConv_byActDay_df30__wagConv_df30)/sum(case when wagConv_byActDay_df30__nDay_df30 is not null then wagConv_byActDay_df30__nDay_df30 else 0 end))
        else 0 end as wagConv_byActDay_df30
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConv_byActDay_df30__wagConv_df30,
        avg(case when playerSeniority<30 then (case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) else NULL end) as wagConv_byActDay_df30__nDay_df30
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as wagConv_byActDay_df30
        """
    },
    'wagConvHist_byActDay': {
        tab_pap_daily_action_pp: """
        case when sum(wagConvHist_byActDay__nDay_hist)>0
        then
        (sum(wagConvHist_byActDay__wagConv_hist)/sum(case when wagConvHist_byActDay__nDay_hist is not null then wagConvHist_byActDay__nDay_hist else 0 end))
        else 0 end as wagConvHist_byActDay
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end) as wagConvHist_byActDay__wagConv_hist,
        avg(case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) as wagConvHist_byActDay__nDay_hist
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as wagConvHist_byActDay
        """
    },
    'wagConvHist_byGameId': {
        #tab_pap_player_profile: """
        #case when count(distinct case when upper(type) IN ('SESSION', 'WAGER') then gameid else null end)>0
        #then
        #(sum(case when upper(type) IN ('SESSION', 'WAGER') then wagHistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER') then gameid else null end))
        #else 0 end
        #as wagConvHist_byGameId
        #""",
        tab_pap_daily_action_aggDay: """
        (sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct objGameId) over (partition by playerId, customerId, clientId order by day)) as wagConvHist_byGameId
        """
    },
    'wagConvHist_byProd': {
        #tab_pap_player_profile: """
        #case when count(distinct case when upper(type) IN ('SESSION', 'WAGER') then prod else null end)>0
        #then
        #(sum(case when upper(type) IN ('SESSION', 'WAGER') then wagHistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER') then prod else null end))
        #else 0 end
        #as wagConvHist_byProd
        #""",
        tab_pap_daily_action_aggDay: """
        (sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnValConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct objProd) over (partition by playerId, customerId, clientId order by day)) as wagConvHist_byProd
        """
    },


    ############ Intensity of Play (wins) ############
    'winConv_byActDay_df07': {
        tab_pap_daily_action_pp: """
        case when sum(winConv_byActDay_df07__nDay_df7)>0
        then
        (sum(winConv_byActDay_df07__winConv_df7)/sum(case when winConv_byActDay_df07__nDay_df7 is not null then winConv_byActDay_df07__nDay_df7 else 0 end))
        else 0 end as winConv_byActDay_df07
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_byActDay_df07__winConv_df7,
        avg(case when playerSeniority<7 then (case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) else NULL end) as winConv_byActDay_df07__nDay_df7
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as winConv_byActDay_df07
        """
    },
    'winConv_byActDay_df14': {
        tab_pap_daily_action_pp: """
        case when sum(winConv_byActDay_df14__nDay_df14)>0
        then
        (sum(winConv_byActDay_df14__winConv_df14)/sum(case when winConv_byActDay_df14__nDay_df14 is not null then winConv_byActDay_df14__nDay_df14 else 0 end))
        else 0 end as winConv_byActDay_df14
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_byActDay_df14__winConv_df14,
        avg(case when playerSeniority<14 then (case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) else NULL end) as winConv_byActDay_df14__nDay_df14
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as winConv_byActDay_df14
        """
    },
    'winConv_byActDay_df30': {
        tab_pap_daily_action_pp: """
        case when sum(winConv_byActDay_df30__nDay_df30)>0
        then
        (sum(winConv_byActDay_df30__winConv_df30)/sum(case when winConv_byActDay_df30__nDay_df30 is not null then winConv_byActDay_df30__nDay_df30 else 0 end))
        else 0 end as winConv_byActDay_df30
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConv_byActDay_df30__winConv_df30,
        avg(case when playerSeniority<30 then (case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) else NULL end) as winConv_byActDay_df30__nDay_df30
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when playerSeniority<=30 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as winConv_byActDay_df30
        """
    },
    'winConvHist_byActDay': {
        tab_pap_daily_action_pp: """
        case when sum(winConvHist_byActDay__nDay_hist)>0
        then
        (sum(winConvHist_byActDay__winConv_hist)/sum(case when winConvHist_byActDay__nDay_hist is not null then winConvHist_byActDay__nDay_hist else 0 end))
        else 0 end as winConvHist_byActDay
        """,
        tab_pap_daily_action_pp_aggDay: """
        sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end) as winConvHist_byActDay__winConv_hist,
        avg(case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) as winConvHist_byActDay__nDay_hist
        """,
        tab_pap_daily_action_aggDay: """
        case when (count(distinct case when upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))>0
        then
        (sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct case when upper(type) IN ('SESSION', 'WAGER') then day else NULL end) over (partition by playerId, customerId, clientId order by day))
        else 0 end
        as winConvHist_byActDay
        """
    },
    'winConvHist_byGameId': {
        #tab_pap_player_profile: """
        #case when count(distinct case when upper(type) IN ('SESSION', 'WAGER') then gameid else null end)>0
        #then
        #(sum(case when upper(type) IN ('SESSION', 'WAGER') then winHistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER') then gameid else null end))
        #else 0 end
        #as winConvHist_byGameId
        #""",
        tab_pap_daily_action_aggDay: """
        (sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct objGameId) over (partition by playerId, customerId, clientId order by day)) as winConvHist_byGameId
        """
    },
    'winConvHist_byProd': {
        #tab_pap_player_profile: """
        #case when count(distinct case when upper(type) IN ('SESSION', 'WAGER') then prod else null end)>0
        #then
        #(sum(case when upper(type) IN ('SESSION', 'WAGER') then winHistconv else 0 end)/count(distinct case when upper(type) IN ('SESSION', 'WAGER') then prod else null end))
        #else 0 end
        #as winConvHist_byProd
        #""",
        tab_pap_daily_action_aggDay: """
        (sum(sum(case when upper(type) IN ('SESSION', 'WAGER') then magnWinConv else 0 end)) over (partition by playerId, customerId, clientId order by day))/(count(distinct objProd) over (partition by playerId, customerId, clientId order by day)) as winConvHist_byProd
        """
    },


    ############ Number of Days of Play ############
    'nday_df07': {
        tab_pap_daily_action_pp: "sum(case when nday_df07_pp is not null then nday_df07_pp else 0 end) as nday_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) as nday_df07_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerSeniority<7 AND upper(type) IN ('SESSION', 'WAGER') then day else null end)) over (partition by playerId, customerId, clientId order by day) as nday_df07"
    },
    'nday_df14': {
        tab_pap_daily_action_pp: "sum(case when nday_df14_pp is not null then nday_df14_pp else 0 end) as nday_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) as nday_df14_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerSeniority<14 AND upper(type) IN ('SESSION', 'WAGER') then day else null end)) over (partition by playerId, customerId, clientId order by day) as nday_df14"
    },
    'nday_df30': {
        tab_pap_daily_action_pp: "sum(case when nday_df30_pp is not null then nday_df30_pp else 0 end) as nday_df30",
        tab_pap_daily_action_pp_aggDay: "avg(case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) as nday_df30_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when playerSeniority<30 AND upper(type) IN ('SESSION', 'WAGER') then day else null end)) over (partition by playerId, customerId, clientId order by day) as nday_df30"
    },
    'nday_hist': {
        tab_pap_daily_action_pp: "sum(case when nday_hist_pp is not null then nday_hist_pp else 0 end) as nday_hist",
        tab_pap_daily_action_pp_aggDay: "avg(case when upper(type) IN ('SESSION', 'WAGER') then 1 else NULL end) as nday_hist_pp",
        tab_pap_daily_action_aggDay: "sum(count(distinct case when upper(type) IN ('SESSION', 'WAGER') then day else null end)) over (partition by playerId, customerId, clientId order by day) as nday_hist"
    },


    # Frequency of Play ############ #Could expand this to have include first
    # two weeks specific metrics
    'd2Play_df07': {
        tab_pap_daily_action_pp: "avg(d2Play_df07) as d2Play_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerSeniority+1)<7 then playerd2play else NULL end) as d2Play_df07",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerSeniority+1)<7 then playerd2play else NULL end)) over (partition by playerId, customerId, clientId order by day) as d2Play_df07"
    },
    'd2PlayGame_df07': {
        tab_pap_daily_action_pp: "avg(d2PlayGame_df07) as d2PlayGame_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerSeniority+1)<7 then playerd2playgame else NULL end) as d2PlayGame_df07",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerSeniority+1)<7 then playerd2playgame else NULL end)) over (partition by playerId, customerId, clientId order by day) as d2PlayGame_df07"
    },
    'd2PlayGameType_df07': {
        tab_pap_daily_action_pp: "avg(d2PlayGameType_df07) as d2PlayGameType_df07",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerSeniority+1)<7 then playerd2playgametype else NULL end) as d2PlayGameType_df07",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerSeniority+1)<7 then playerd2playgametype else NULL end)) over (partition by playerId, customerId, clientId order by day) as d2PlayGameType_df07"
    },
    'd2Play_df14': {
        tab_pap_daily_action_pp: "avg(d2Play_df14) as d2Play_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerSeniority+1)<14 then playerd2play else NULL end) as d2Play_df14",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerSeniority+1)<14 then playerd2play else NULL end)) over (partition by playerId, customerId, clientId order by day) as d2Play_df14"
    },
    'd2PlayGame_df14': {
        tab_pap_daily_action_pp: "avg(d2PlayGame_df14) as d2PlayGame_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerSeniority+1)<14 then playerd2playgame else NULL end) as d2PlayGame_df14",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerSeniority+1)<14 then playerd2playgame else NULL end)) over (partition by playerId, customerId, clientId order by day) as d2PlayGame_df14"
    },
    'd2PlayGameType_df14': {
        tab_pap_daily_action_pp: "avg(d2PlayGameType_df14) as d2PlayGameType_df14",
        tab_pap_daily_action_pp_aggDay: "avg(case when (playerSeniority+1)<14 then playerd2playgametype else NULL end) as d2PlayGameType_df14",
        tab_pap_daily_action_aggDay: "avg(avg(case when (playerSeniority+1)<14 then playerd2playgametype else NULL end)) over (partition by playerId, customerId, clientId order by day) as d2PlayGameType_df14"
    },
    'd2Play_hist': {
        tab_pap_daily_action_pp: "avg(playerd2play) as d2Play_hist",
        tab_pap_daily_action_pp_aggDay: "avg(playerd2play) as playerd2play",
        tab_pap_daily_action_aggDay: "avg(avg(playerd2play)) over (partition by playerId, customerId, clientId order by day) as d2Play_hist"
    },
    'd2PlayGame_hist': {
        tab_pap_daily_action_pp: "avg(playerd2playgame) as d2PlayGame_hist",
        tab_pap_daily_action_pp_aggDay: "avg(playerd2playgame) as playerd2playgame",
        tab_pap_daily_action_aggDay: "avg(avg(playerd2playgame)) over (partition by playerId, customerId, clientId order by day) as d2PlayGame_hist"
    },
    'd2PlayGameType_hist': {
        tab_pap_daily_action_pp: "avg(playerd2playgametype) as d2PlayGameType_hist",
        tab_pap_daily_action_pp_aggDay: "avg(playerd2playgametype) as playerd2playgametype",
        tab_pap_daily_action_aggDay: "avg(avg(playerd2playgametype)) over (partition by playerId, customerId, clientId order by day) as d2PlayGameType_hist"
    }
}


def getDF_playerProf(
        sqlContext,
        where_pp=None,
        where_da=None,
        where_inner_da=None,
        features=[],
        nDailyActions="1000000"):
    '''
    1. Look for direct player profile
    2. If not, then look for the customized kpi built for player profile from 
       daily_action_aggDay
    3. If not, then look if there is a generic daily_action that can be used, 
       getting data always from daily_action_aggDay
    '''
    db_player_profile_list = []
    db_player_profile_other_list = []
    db_daily_action_list = []
    db_daily_action_list_kpi = []
    db_daily_action_aggDay_list = []
    select_player_profile = ''
    select_player_profile_other = ''
    select_daily_action = ''
    select_daily_action_aggDay = ''

    for kpi, dic in player_data.iteritems():
        if(len(features) == 0) | ((len(features) != 0) & (kpi in features)):
            if(tab_pap_player_profile in dic):
                db_player_profile_list.append(dic[tab_pap_player_profile])
            elif(tab_pap_player_profile_other in dic):
                print(col)
                db_player_profile_other_list.append(
                    dic[tab_pap_player_profile_other])
            elif(tab_pap_daily_action_pp in dic):
                db_daily_action_list.append(dic[tab_pap_daily_action_pp])
                if(tab_pap_daily_action_pp_aggDay in dic):
                    db_daily_action_list_kpi.append(kpi)

    if(len(db_player_profile_list) > 0):
        select_player_profile = (
            "select playerid, customerid, clientid, " +
            ', '.join(db_player_profile_list) +
            ' from player_profile ' +
            'where waghistconv>0 ' +
            (
                ' and (' +
                where_pp +
                ') ' if where_pp is not None else '') +
            'group by playerid, customerid, clientid')

    if(len(db_player_profile_other_list) > 0):
        print("Entered")
        select_player_profile_other = (
            "select playerid, customerid, clientid, " +
            ', '.join(db_player_profile_list) +
            ' from player_profile_other ' +
            'where 1=1 ' +
            (
                ' and (' +
                where_pp +
                ') ' if where_pp is not None else '') +
            'group by playerid, customerid, clientid')

    if(len(db_daily_action_list) > 0):
        dataDailyAction = getDF_playerTS(
            sqlContext,
            ts_period='d',
            where_da=where_da,
            where_inner_da=where_inner_da,
            features=db_daily_action_list_kpi,
            nDailyActions=nDailyActions,
            isSubQuery=True)
        dataDailyAction.registerTempTable('daily_action_aggDay')

        select_daily_action = (
            "select playerid, customerid, clientid, " +
            ', '.join(db_daily_action_list) +
            ' from daily_action_aggDay '
            'group by playerid, customerid, clientid')

    if(select_player_profile != ''):
        if(select_daily_action != ''):
            df_pp = sqlContext.sql(select_player_profile)
            df_da = sqlContext.sql(select_daily_action)
            df = (
                df_pp.join(
                    df_da, [
                        'playerid', 'customerid', 'clientid'], 'inner') .drop(
                    df_da["playerid"]) .drop(
                    df_da["clientid"]) .drop(
                        df_da["customerid"]) .select('*'))
        else:
            df = sqlContext.sql(select_player_profile)
    else:
        df = sqlContext.sql(select_daily_action)

    sqlContext.dropTempTable('daily_action_aggDay')

    return df


def getDF_playerTS(
        sqlContext,
        ts_period='d',
        where_da=None,
        where_inner_da=None,
        features=[],
        nDailyActions="1000000",
        isSubQuery=False):

    # doServiceFeat will create "service features" in case this is used as
    # input table for player profile
    serviceFeat = '''
        sum(magnvalconv) as magnvalconv,
        sum(magnwinconv) as magnwinconv,
        max(playerseniority) as playerseniority
    '''
    db_daily_action_list = []
    select_daily_action = ''

    print('ecco')
    for kpi, dic in player_data.iteritems():
        if((len(features) == 0) & (not isSubQuery)) | ((len(features) != 0) & (kpi in features)):

            # define the Hierarchy for which table should be used when multiple
            # definition of kpis are used
            if(isSubQuery):
                if(tab_pap_daily_action_pp_aggDay in dic):
                    db_daily_action_list.append(
                        dic[tab_pap_daily_action_pp_aggDay])
                elif(tab_pap_daily_action_aggDay in dic):
                    db_daily_action_list.append(
                        dic[tab_pap_daily_action_aggDay])
            else:
                if(tab_pap_daily_action_aggDay in dic):
                    db_daily_action_list.append(
                        dic[tab_pap_daily_action_aggDay])
                elif(tab_pap_daily_action in dic):
                    db_daily_action_list.append(dic[tab_pap_daily_action])

    if(len(db_daily_action_list) > 0 or isSubQuery):
        print()
        select_daily_action = ("select * from ( " +
                               "select playerid, customerid, clientid, day " +
                               ((', ' +
                                 serviceFeat) if isSubQuery else '') +
                               ((', ' +
                                 ', '.join(db_daily_action_list)) if len(db_daily_action_list) > 0 else '') +
                               ' from ( ' +
                               ' select * ' +
                               " from daily_action " +
                               'where 1=1 ' +
                               (' and (' +
                                   where_inner_da +
                                   ') ' if where_inner_da is not None else '') +
                               ' ) a ' +
                               'group by playerid, customerid, clientid, day, playerseniority ' +
                               ' ) b ' +
                               'where 1=1 ' +
                               (' and (' +
                                   where_da +
                                   ') ' if where_da is not None and not isSubQuery else '') +
                               ('limit ' +
                                   nDailyActions if int(nDailyActions) > 0 else ''))

    if(select_daily_action != ''):
        df = sqlContext.sql(select_daily_action)

    return df


# Should probably remove this? Not sure why it needs to be a separate func.
def getDF_query(sqlContext, query, tableName):
    df = sqlContext.sql(query)
    return df
