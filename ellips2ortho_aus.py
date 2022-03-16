import pandas as pd
import math
import numpy
import rasterio.sample

file_path = './aus_geotags.csv'
geoid09_file = './au_ga_AUSGeoid09_V1.01.tif'
geoid20_file = './au_ga_AUSGeoid2020_20170908.tif'

def geo_to_cart(lat, lon, h):
    lat_rad = (lat/180)*math.pi
    lon_rad = (lon/180)*math.pi
    
    a = 6378137.00
    f = 1/298.257222101
    
    e2 = 2*f - f*f
    v = a/(math.sqrt(1 - e2*math.sin(lat_rad)*math.sin(lat_rad)))

    x = (h + v)*math.cos(lat_rad)*math.cos(lon_rad)
    y = (h + v)*math.cos(lat_rad)*math.sin(lon_rad)
    z = ((1 - e2)*v + h)*math.sin(lat_rad)
    
    return x,y,z

def cart_to_cart(x0, y0, z0):
    Tx = 0.06155
    Ty = -0.01087
    Tz = -0.04019
    Sc = -0.009994
    Rx = -0.0394924
    Ry = -0.0327221
    Rz = -0.0328979
    
    RxRad = ((Rx/3600)/180)*math.pi
    RyRad = ((Ry/3600)/180)*math.pi
    RzRad = ((Rz/3600)/180)*math.pi
    Scale = 1 + (Sc/1000000)
    
    T = numpy.zeros(shape=(3,1))
    T[0][0] = Tx
    T[1][0] = Ty
    T[2][0] = Tz
    
    R = numpy.zeros(shape=(3,3))
    R[0][0] = math.cos(RyRad)*math.cos(RzRad)
    R[0][1] = math.cos(RyRad)*math.sin(RzRad)
    R[0][2] = -math.sin(RyRad)
    
    R[1][0] = math.sin(RxRad)*math.sin(RyRad)*math.cos(RzRad) - math.cos(RxRad)*math.sin(RzRad) 
    R[1][1] = math.sin(RxRad)*math.sin(RyRad)*math.sin(RzRad) + math.cos(RxRad)*math.cos(RzRad) 
    R[1][2] = math.sin(RxRad)*math.cos(RyRad)

    R[2][0] = math.cos(RxRad)*math.sin(RyRad)*math.cos(RzRad) + math.sin(RxRad)*math.sin(RzRad) 
    R[2][1] = math.cos(RxRad)*math.sin(RyRad)*math.sin(RzRad) - math.sin(RxRad)*math.cos(RzRad) 
    R[2][2] = math.cos(RxRad)*math.cos(RyRad)
    
    Xold = numpy.zeros(shape=(3,1))
    Xold[0][0] = x0
    Xold[1][0] = y0
    Xold[2][0] = z0

    Xnew = T + Scale*numpy.matmul(R, Xold)
    
    return Xnew[0][0], Xnew[1][0], Xnew[2][0]

def cart_to_geo(x1, y1, z1):
    a = 6378137.00
    f = 1/298.257222101
    
    e2 = 2*f - f*f
    p = math.sqrt(x1*x1 + y1*y1)
    r = math.sqrt(p*p + z1*z1)
    u = math.atan((z1/p)*((1-f) + (e2*a)/r))
    
    lat_top = z1*(1-f) + e2*a*math.sin(u)*math.sin(u)*math.sin(u)
    lat_bot = (1-f)*(p - e2*a*math.cos(u)*math.cos(u)*math.cos(u))
    
    lon_rad = math.atan(y1/x1)
    if lon_rad < 0:
        lon = 180*(math.pi + lon_rad)/math.pi
    else:
        lon = 180*lon_rad/math.pi
    
    lat_rad = math.atan(lat_top/lat_bot)
    lat = 180*lat_rad/math.pi
    
    h = p*math.cos(lat_rad) + z1*math.sin(lat_rad) - a*math.sqrt(1 - e2*math.sin(lat_rad)*math.sin(lat_rad))

    return lat, lon, h

def gda94_to_gda2020(lat, lon, h):
    x, y, z = geo_to_cart(lat, lon, h)
    x1, y1, z1 = cart_to_cart(x, y, z)
    
    return cart_to_geo(x1, y1, z1)
    
df = pd.read_csv(file_path, index_col=False)

lat = 'latitude [decimal degrees]'
lon = 'longitude [decimal degrees]'
height = 'altitude [meter]'
    
# Geoid Selection

geoid_dict = {1: 'Aus Geoid 09', 2: 'Aus Geoid 2020'}

geoid = input('Please select geoid: ')

if geoid=='1':
    ortho = []
    geoid09 = rasterio.open(geoid09_file)
    points = list(zip(df[lon].tolist(), df[lat].tolist()))
    
    i = 0
    for val in geoid09.sample(points):
        ortho.append(df[height][i] - val[0])
        i += 1
    
    df[height] = ortho
    df.rename(columns={lat: 'latitude GDA94 [decimal degrees]',
                       lon: 'longitude GDA94 [decimal degrees]',
                       height: 'orthometric height [meters]'}, inplace=True)

else:
    ortho = []
    geoid20 = rasterio.open(geoid20_file)
    
    # Convert Coordinates
    lat_gda20 = []
    lon_gda20 = []
    h_gda20 = []

    for x in range(len(df[lat])):
        la, lo, h = gda94_to_gda2020(df[lat][x], df[lon][x], df[height][x])
        lat_gda20.append(la)
        lon_gda20.append(lo)
        h_gda20.append(h)
    
    points = list(zip(lon_gda20,lat_gda20))
    
    i = 0
    
    for val in geoid20.sample(points):
        ortho.append(h_gda20[i] - val[0])
        i += 1
    
    df[lat] = lat_gda20
    df[lon] = lon_gda20
    df[height] = ortho
    
    df.rename(columns={lat: 'latitude GDA20 [decimal degrees]',
                       lon: 'longitude GDA20 [decimal degrees]',
                       height: 'orthometric height [meters]'}, inplace=True)

df.to_csv('aus_geotags_orthometric.csv', index=False)