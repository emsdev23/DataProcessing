﻿import requests
import time


headers = {'Authorization': 'VKOnNhH2SebMU6S'}

windmurl = 'http://localhost:8000/windMonth'
winddurl = 'http://localhost:8000/windDayWise'
ltoenurl = 'http://localhost:8000/ltoEnergy'
ioeenurl = 'http://localhost:8000/ioeEnergy'
sltenurl = 'http://localhost:8000/slotWiseEnergy'
sltclurl = 'http://localhost:8000/slotWiseCalculation'
gddurlp = 'http://localhost:8000/gridDaywiseprev'
gddurl = 'http://localhost:8000/gridDaywise'
kvaurl = 'http://localhost:8000/kvavskwh'
winmurl = 'http://localhost:8000/windEnergyHourly'
winhurl = 'http://localhost:8000/windHourly'
ioehurl = 'http://localhost:8000/ioeHourly'
clturl = 'http://localhost:8000/clientpower'
mxpurl = 'http://localhost:8000/maxpeakjmp'
atfurl = 'http://localhost:8000/ablktf'
btfurl = 'http://localhost:8000/bblktf'
ctfurl = 'http://localhost:8000/cblktf'
d3furl = 'http://localhost:8000/dblk3f'
d7furl = 'http://localhost:8000/dblk7f'
mlcpurl = 'http://localhost:8000/mlcpgf'
eblk9url = 'http://localhost:8000/eblockgf9f'
eblk1url = 'http://localhost:8000/eblock1f'
tstmpurl = 'http://localhost:8000/tsstoredwater'
invurl2 = 'http://localhost:8000/inverterhourph2'
invurl = 'http://localhost:8000/inverterhour'
elcurl = 'http://localhost:8000/electricDayWise'
upsurl = 'http://localhost:8000/upsHourly'
wheelurl = 'http://localhost:8000/wheeledhourlyph1'
wheelurl2 = 'http://localhost:8000/wheeledhourlyph2'
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
bmsurl = 'http://localhost:8000/bmsgridhourly'
thrmlstsurl = 'http://localhost:8000/thermalstatus'
peakqurl = 'http://localhost:8000/peakquarter'
demand = 'http://localhost:8000/demandvsgrid'
ltourl = 'http://localhost:8000/ltohourly'
dgurl = 'http://localhost:8000/dghourly'
dgqurl = 'http://localhost:8000/dgquarterly'

def windmhr():
    print("Wind Month Wise Energy")
    upsres = requests.get(windmurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def winddhr():
    print("Wind Day Wise Energy")
    upsres = requests.get(winddurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def ltoenhr():
    print("LTO Energy")
    upsres = requests.get(ltoenurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def ioeenhr():
    print("IOE Energy")
    upsres = requests.get(ioeenurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def stclhr():
    print("Slot Wise Calculation")
    upsres = requests.get(sltclurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def stenhr():
    print("Slot Wise Energy")
    upsres = requests.get(sltenurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def gddphr():
    print("Grid Day Wise")
    upsres = requests.get(gddurlp, headers=headers)
    print(upsres.json())
    time.sleep(10)

def gddhr():
    print("Grid Day Wise")
    upsres = requests.get(gddurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def kvahr():
    print("KVA vs KWh")
    upsres = requests.get(kvaurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def winmhr():
    print("Wind energy hourly")
    upsres = requests.get(winmurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def winhr():
    print("Wind hourly")
    upsres = requests.get(winhurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def ioehr():
    print("Ioe hourly")
    upsres = requests.get(ioehurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def cltfn():
    print("Client Power")
    upsres = requests.get(clturl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def maxpjf():
    print("Max Peak jump")
    upsres = requests.get(mxpurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def ablktf():
    print("A block terrace")
    upsres = requests.get(atfurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def bblktf():
    print("B block terrace")
    upsres = requests.get(btfurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def cblktf():
    print("C block terrace")
    upsres = requests.get(ctfurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def dblk7f():
    print("D block 7th floor")
    upsres = requests.get(d7furl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def dblk3f():
    print("D block 3rd floor")
    upsres = requests.get(d3furl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def mlcpgf():
    print("MLCP 0th floor")
    upsres = requests.get(mlcpurl, headers=headers)
    print(upsres.json())
    time.sleep(10)

def eblk9f():
    print("Eblock 0th & 9th floor")
    upsres = requests.get(eblk9url, headers=headers)
    print(upsres.json())
    time.sleep(10)

def eblk1f():
    print("Eblock 1st floor")
    upsres = requests.get(eblk1url, headers=headers)
    print(upsres.json())
    time.sleep(10)

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

def invhr2():
    print("Inverter hourly2")
    upsres = requests.get(invurl2, headers=headers)
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

def wheel2():
    print("WHEELED")
    wheelres = requests.get(wheelurl2, headers=headers)
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


function_names = ['windmhr','ltoenhr','wheel','wheel2']

# 'winmhr','ltohr','grid','winhr','ioehr','wheel2','invhr2','wheel','maxpjf','invhr','tstemp','elecdy','griddaily','dgqtr','dghr','upshr','thermal','roof','dghour','thermalq','bms','peak','peakq','power','thermalstatus','roofd','peakd'

# 'cltfn','ablktf','bblktf','cblktf','dblk3f','dblk7f','mlcpgf','eblk9f','eblk1f'
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
