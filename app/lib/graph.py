import shapely.wkt


def str_to_latlong(x):
    P = shapely.wkt.loads(x)
    return (P.centroid.y, P.centroid.x)
