platform_init = {
    # 定義初始化日期
    "init_start_date": "2023/01/10 00:00:00",
    # 要執行的平台名稱
    "platform": "ALL",
    # 要執行的站點名稱
    "site_code": "ALL",
    # 要執行的遊戲名稱
    "game_code": "ALL",
}

current_init = {
    "risk_ctrl": [
        {
            "assignee": "risk_ctrl_player_1d",
            "freq_type": "1D",
            "level": 200
        },
        {
            "assignee": "risk_ctrl_game_1d",
            "freq_type": "1D",
            "level": 200
        },
        {
            "assignee": "risk_ctrl_rpt_1d",
            "freq_type": "1D",
            "level": 200
        }
    ]
}

all_report_list = {
    "player_summary": [
        {
            "assignee": "player_summary_5min",
            "freq_type": "5min",
            "level": 100
        },
        {
            "assignee": "player_summary_1h",
            "freq_type": "1H",
            "level": 200
        },
        {
            "assignee": "player_summary_1d",
            "freq_type": "1D",
            "level": 300
        },
        {
            "assignee": "player_summary_1m",
            "freq_type": "1M",
            "level": 400
        }
    ]
    ,
    "trans_summary": [
        {
            "assignee": "trans_summary_5min",
            "freq_type": "5min",
            "level": 100
        },
        {
            "assignee": "trans_summary_1h",
            "freq_type": "1H",
            "level": 200
        },
        {
            "assignee": "trans_summary_1d",
            "freq_type": "1D",
            "level": 300
        },
        {
            "assignee": "trans_summary_m",
            "freq_type": "1M",
            "level": 400
        },
    ],
    "new_register_summary": [
        {
            "assignee": "new_register_summary_1d",
            "freq_type": "1D",
            "level": 300
        },
    ],
    "risk_ctrl": [
        {
            "assignee": "risk_ctrl_player_1d",
            "freq_type": "1D",
            "level": 300
        },
        {
            "assignee": "risk_ctrl_game_1d",
            "freq_type": "1D",
            "level": 300
        }
    ],
}
