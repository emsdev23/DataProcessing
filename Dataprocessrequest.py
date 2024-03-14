import requests
import time


headers = {'Authorization': 'VKOnNhH2SebMU6S'}

tstmpurl = 'http://localhost:8000/tsstoredwater'
invurl = 'http://localhost:8000/inverterhour'
elcurl = 'http://localhost:8000/electricDayWise'
upsurl = 'http://localhost:8000/upsHourly'
wheelurl = 'http://localhost:8000/wheeledhourly'
gridurl = 'http://localhost:8000/gridhourly'
griddurl = 'http://localhost:8000/griddaily'
peakurl = 'http://localhost:8000/peakdemandhourly'
roofurl = 'http://localhost:8000/rooftopHourly'
roofdailyurl = 'http://localhost:8000/rooftopdaily'
thermalurl = 'http://localhost:8000/thermalhourly'
thermalqurl = 'http://localhost:8000//thermalquarterl'
peakdurl='http://localhost:8000/peakdaily'
energyurl = 'http://localhost:8000/enrgysaved'
powerurl = 'http://localhost:8000/powerfactorhourly'
wheelurl = 'http://localhost:8000/wheeledhourly'
bmsurl = 'http://localhost:8000/bmsgridhourly'
thrmlstsurl = 'http://localhost:8000/thermalstatus'
peakqurl = 'http://localhost:8000/peakquarter'
demand = 'http://localhost:8000/demandvsgrid'
ltourl = 'http://localhost:8000/ltohourly'
dgurl = 'http://localhost:8000/dghourly'
dgqurl = 'http://localhost:8000/dgquarterly'


def tstemp():
    print("TS Stored Water quarterly")
    upsres = requests.get(tstmpurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def invhr():
    print("Inverter hourly")
    upsres = requests.get(invurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def elecdy():
    print("Electric Day wise")
    upsres = requests.get(elcurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def dgqtr():
    print("DG quarterly")
    upsres = requests.get(dgqurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def upshr():
    print("UPS hourly")
    upsres = requests.get(upsurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def dghr():
    print("DG hourly")
    gridres = requests.get(dgurl, headers=headers)
    print(gridres.json())
    time.sleep(10)

def grid():
    print("GRID")
    gridres = requests.get(gridurl, headers=headers)
    print(gridres.json())
    time.sleep(10)

def ltohr():
    print("LTO hourly")
    gridres = requests.get(ltourl, headers=headers)
    print(gridres.json())
    time.sleep(10)

def wheel():
    print("WHEELED")
    wheelres = requests.get(wheelurl, headers=headers)
    print(wheelres.json())
    time.sleep(10)

def griddaily():
    print("GRID DAILY")
    griddres = requests.get(griddurl, headers=headers)
    print(griddres.json())
    time.sleep(10)
    
def bms():
    print("BMS GRID")
    bmsres = requests.get(bmsurl, headers=headers)
    print(bmsres.json())
    time.sleep(10)
   
def wheel():
    print("WHEELED-IN")
    wheelres = requests.get(wheelurl, headers=headers)
    print(wheelres.json())
    time.sleep(10)
   
def peak():
    print("PEAK")
    peakres= requests.get(peakurl, headers=headers)
    print(peakres.json())
    time.sleep(10)
   
def roof():
    print("ROOF")
    roofres= requests.get(roofurl, headers=headers)
    print(roofres.json())
    time.sleep(10)
   
def roofd():
    print("ROOF DAILY")
    roofdailyres= requests.get(roofdailyurl, headers=headers)
    print(roofdailyres.json())
    time.sleep(10)

def thermal():
    print("THERMAL")
    thermalres = requests.get(thermalurl, headers=headers)
    print(thermalres.json())
    time.sleep(10)

def thermalq():
    print("THERMAL")
    thermalqres = requests.get(thermalqurl, headers=headers)
    print(thermalqres.json())
    time.sleep(10)

def peakd():
    print("PEAK DAILY")
    peakdres = requests.get(peakdurl, headers=headers)
    print(peakdres.json())
    time.sleep(10)

def peakq():
    print("PEAK QUARTER")
    peakqres = requests.get(peakqurl, headers=headers)
    print(peakqres.json())
    time.sleep(10)

def energy():
    print("ENERGY")
    energyres = requests.get(energyurl, headers=headers)
    print(energyres.json())
    time.sleep(10)
  
def power():
    print("POWER FACTOR")
    powerres = requests.get(powerurl, headers=headers)
    print(powerres.json())
    time.sleep(10)
    
def thermalstatus():
    print("THERMAL STATUS")
    thrres = requests.get(thrmlstsurl, headers=headers)
    print(thrres.json())
    time.sleep(10)

def demand():
    print("PEAK VS GRID")
    demres = requests.get(demand, headers=headers)
    print(demres.json())
    time.sleep(10)


function_names = ['upshr']
    #'thermalq','grid','bms','peak','ltoHour','griddaily','peakq','thermal','wheel','roof','thermalstatus','roofd','peakd','demandp']

while True:
    for name in function_names:
        if name in globals() and callable(globals()[name]):
            function = globals()[name]
            try:
                result = function()
            except:
                continue
        else:
            print(f"Function {name} not found.")    
   
    time.sleep(180)
