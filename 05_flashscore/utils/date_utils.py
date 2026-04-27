from datetime import datetime, timedelta

def resolver_data(data_str: str):
    if data_str == "yesterday":
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if data_str == "today":
        return datetime.now().strftime("%Y-%m-%d")

    return data_str