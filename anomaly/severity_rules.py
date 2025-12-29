def completeness_severity(delta_pct):
    if delta_pct >= 5:
        return "HIGH"
    elif delta_pct >= 2:
        return "MEDIUM"
    elif delta_pct >= 0.5:
        return "LOW"
    return None

def volume_severity(delta_pct):
    if delta_pct >= 30:
        return "HIGH"
    elif delta_pct >= 10:
        return "MEDIUM"
    elif delta_pct >= 5:
        return "LOW"
    return None

def distribution_severity(z):
    if abs(z) >= 3:
        return "HIGH"
    elif abs(z) >= 2:
        return "MEDIUM"
    elif abs(z) >= 1.5:
        return "LOW"
    return None

def referential_severity(rate):
    if rate >= 1:
        return "HIGH"
    elif rate >= 0.5:
        return "MEDIUM"
    elif rate >= 0.1:
        return "LOW"
    return None
