"""
env setting
"""
# run_type = 'run_schedule' 按照schedule排程執行
# run_type = 'run_once' 全部跑一次並結束
run_type = 'run_once'

# Telegram message title
tg_message_title = 'OA'

# timezone
utc_timezone_switch = False

"""
risk_ctrl series report setting
"""
risk_ctrl_1d_config = {
    'target_conn_report': 'get_report_db_maria_conn',
    'default_value_player': {
        'def_b_wma': 5000000,  # 5B
        'def_b_wmasd': 5000000,  # 5B
        'def_p_wma': 1000000,  # 1B
        'def_p_wmasd': 1000000,  # 1B
        'def_trans_wma': 5000000,  # 5B
        'def_trans_wmasd': 5000000,  # 5B
    },
    'default_value_game': {
        'def_b_wma': 50000000,  # 50B
        'def_b_wmasd': 50000000,  # 50B
        'def_p_wma': 10000000,  # 10B
        'def_p_wmasd': 10000000,  # 10B
        'def_trans_wma': 50000000,  # 50B
        'def_trans_wmasd': 50000000,  # 50B
    },
    'country_rate': {
        'THB': 0.05,
        'VND2': 1,
        'INR': 1,
        'PHP': 1,
    },
    'risk_score_multi': {
        'b': 10,
        'wl': 50,
        'trans': 25,
        'p_trans_multi': 100
    },
    'rtp_cfg_player': {
        'profit_threshold': 10000,
        'profit_unconditional': 50000,
        'rtp_threshold': 1,
        'bet_count_threshold': 10,
    },
    'rtp_cfg_game': {
        'profit_threshold': 10000,
        'profit_unconditional': 50000,
        'rtp_threshold': 1,
        'bet_count_threshold': 10,
    },
    'rtp': {
        'profit_threshold': 10000,
        'profit_unconditional': 50000,
        'rtp_threshold': 1,
        'bet_count_threshold': 10,
        'time_to_alert':1, # 每次警報間隔
        'alert_limit':5, # 最高警報次數
    },
}

"""
player_summary series report setting
"""
player_summary_rtp_config = {
    'profit_threshold': 10000,
    'profit_unconditional': 50000,
    'rtp_threshold': 1,
    'bet_count_threshold': 10,
}

player_summary_5min_config = {
    'source_conn_tg_admin': 'get_source_db_cr_tg_admin_conn',
    'target_conn_report': 'get_report_db_maria_conn'
}
player_summary_1h_config = {
    'source_conn': 'get_report_db_maria_conn',
    'target_conn': 'get_report_db_maria_conn'
}
player_summary_1d_config = {
    'source_conn': 'get_report_db_maria_conn',
    'target_conn': 'get_report_db_maria_conn'
}
player_summary_1m_config = {
    'source_conn': 'get_report_db_maria_conn',
    'target_conn': 'get_report_db_maria_conn'
}
new_register_summary_1d_config = {
    'source_conn': 'get_source_db_cr_gs_conn',
    'target_conn': 'get_report_db_maria_conn'
}

"""
trans_summary series report setting
"""
trans_summary_5min_config = {
    'source_conn_gs': 'get_source_db_cr_gs_conn',
    'target_conn_report': 'get_report_db_maria_conn'
}
trans_summary_1h_config = {
    'source_conn_report': 'get_report_db_maria_conn',
    'target_conn_report': 'get_report_db_maria_conn'
}
trans_summary_1d_config = {
    'source_conn_report': 'get_report_db_maria_conn',
    'target_conn_report': 'get_report_db_maria_conn'
}
trans_summary_1m_config = {
    'source_conn_report': 'get_report_db_maria_conn',
    'target_conn_report': 'get_report_db_maria_conn'
}
