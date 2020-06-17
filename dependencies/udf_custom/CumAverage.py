import redis


def _anomaly_cum_average(count, weektime, *identifiers, config):
    is_anomaly = False
    trust_value, alpha, group_by, model_identifier = config["process_cum_trust_value"], \
                                                     config["process_cum_alpha"], \
                                                     config["group_by"], \
                                                     config["model_identifier"]

    redis_host = config["redis_host"]
    r = redis.Redis(host=redis_host)

    key_prefix = "{}:{}".format(model_identifier, ':'.join(i or "None" for i in identifiers))

    count = count
    weektime = weektime
    key_ema = "{}:{}:{}".format(key_prefix, weektime, "ema")
    key_emd = "{}:{}:{}".format(key_prefix, weektime, "emd")
    beta = 1 - alpha

    print("processing row with key ema: {}, emd: {}".format(key_ema, key_emd))

    db_ema = r.get(key_ema)
    db_emd = r.get(key_emd)
    if db_ema is None:
        ema = count
        emd = 0
    else:
        db_ema = float(db_ema)
        db_emd = float(db_emd)

        upper_bound = db_ema + trust_value * db_emd

        print("count: {}, db_ema: {}, db_emd: {}, upper_b: {}".format(count, db_ema, db_emd,
                                                                                   upper_bound))

        ema = db_ema * beta + alpha * count
        emd = beta * (db_emd + alpha * (count - ema) ** 2)

        is_anomaly = count > upper_bound

    r.set(key_ema, ema)
    r.set(key_emd, emd)

    return is_anomaly
