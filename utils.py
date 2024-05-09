
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta


async def raise_date(group_type: str, date: str) -> str:
    '''Увеличение даты в зависимости от типа группировки.'''

    if group_type == 'hour':
        raised_date: str = (datetime.fromisoformat(date)
                            + timedelta(hours=1)
                            ).isoformat()
    elif group_type == 'day':
        raised_date: str = (datetime.fromisoformat(date)
                            + timedelta(days=1)
                            ).isoformat()
    elif group_type == 'month':
        raised_date: str = (datetime.fromisoformat(date)
                            + relativedelta(months=1)
                            ).isoformat()
    return raised_date
