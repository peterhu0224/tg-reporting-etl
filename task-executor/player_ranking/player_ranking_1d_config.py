from datetime import datetime, timedelta


class VndOuConfig:
    """
    越式大小排行榜
    """
    # 設定重跑起始日期
    VND_OU_RERUN_START_DATE = '2023-01-01'
    # 設定重跑結束日期
    VND_OU_RERUN_END_DATE = '2023-01-02'
    # 預設為0，若重跑需要初始值，可調整為>0的數字
    VND_OU_DEFAULT_MAX_VALUE = 0
    # 機器人中獎最低人數
    VND_OU_BONUS_RELEASE_FLOOR = 3
    # 機器人中獎最高人數
    VND_OU_BONUS_RELEASE_CEIL = 8
    # 越式大小排行榜維護清單
    VND_OU_LEADERBOARD_LIST = [
        # UFA清單
        {'platform': 'UFA',
         'site_code': ['TG', 'UAT', "TEST"],
         'game_code': ['05'],
         'room_type': ['1001', '1002'],
         'country': ['INR', 'THB', 'PHP', 'VND2']
         },
        # GEA清單
        {'platform': 'GEA',
         'site_code': ['UAT'],
         'game_code': ['05'],
         'room_type': ['1001', '1002'],
         'country': ['INR', 'THB', 'PHP', 'VND2']
         }
    ]
