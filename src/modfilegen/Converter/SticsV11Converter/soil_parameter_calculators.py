def calculate_q0(soil):
    """Calculate the STICS q0 parameter from the soil texture."""
    sand = float(soil["Sand"])
    clay = float(soil["Clay"])

    if sand > 80.0:
        return 5.0 + 0.15 * (100.0 - sand)
    if clay > 50.0:
        return 5.0 + 0.06 * (100.0 - clay)
    return 8.0 + 0.08 * clay
