#!/usr/bin/python
#bip flume source 
import time
import threading
import os
import sys
import stat
import cPickle as pickle 

Fd = None
now_position = 0

def prevent_deadsubprocess( pid ):
    if os.getpid() == 1:
        os.system( "kill -9 " + str( pid ))
        return
    t = threading.Timer( 10, prevent_deadsubprocess, [ pid ])
    t.start()    

def cron( start, interval, count, offset_file ):        #Save now offset when interval comming
    global now_position
    ticks = time.time()
    t = threading.Timer ( start + interval * ( count + 1 ) -ticks, cron, [ start, interval, count+1, offset_file ] )
    t.start()
    File_offset = open( offset_file, 'w' )
    save_dict = {}
    save_dict[ 'offset' ] = now_position
    Localtime = time.localtime()
    save_dict[ 'year' ] = time.strftime( '%Y', Localtime )
    save_dict[ 'month' ] = time.strftime( '%m', Localtime )
    save_dict[ 'day' ] = time.strftime( '%d', Localtime )
    save_dict[ 'hour' ] = time.strftime( '%H', Localtime )
    dumps = pickle.dumps( save_dict )
    File_offset.write(dumps)
    File_offset.close()

def readfile( filename, offset, offset_file ):
    try:
        global now_position
        Fd = open( filename, 'r' )
        file_status = os.stat( filename )
        file_inode = file_status[ stat.ST_INO ]
        last_position = offset
        Fd.seek( last_position, 0 )
        t = threading.Timer( 1, cron, [ time.time(), 10, 0, offset_file ])
        t.start()
        brake = 0
        while True:
            if brake > 1000:
                time.sleep( 0.1 )
                brake = 0
            line = Fd.readline()
            now_position = Fd.tell()
            if last_position == now_position:    #No data comming Or file rotate
                brake = 0
                time.sleep( 0.1 )
                while True:                     #prevent file not exists in rotating
                    if os.path.exists( filename ):
                        break
                    time.sleep( 0.1 )
                file_status = os.stat( filename )
                if file_status[ stat.ST_INO ] != file_inode: #if file_inode not same, cause by file rotated
                    time.sleep(0.5)             #wait for nginx reload, some datastream could be input old file
                    while True:             #consumer all data from the old file
                        if brake > 1000:
                            time.sleep( 0.1 )
                            brake = 0
                        line = Fd.readline()
                        now_position = Fd.tell()
                        if now_position == last_position:  #I think it express read over when position same agen!
                            Fd.close()
                            Fd = open( filename, 'r' )
                            file_inode = file_status[ stat.ST_INO ]
                            last_position = 0
                            break
                        print line,
                        brake += 1
                        last_position = now_position
                continue
            print line,
            brake += 1
            last_position = now_position
    except Exception as e:
        print e            
    finally:
            Fd.close()          
def new_day ( year, month, day, hour):
    big_month = [ 1, 3, 5, 7, 8, 10, 12 ]       # max days 31
    hour += 1
    if hour == 24:
        day += 1
        hour = 0
    if month == 2 :         # february 
        if   year%400 == 0 or (year % 4 ==0 and year % 100 != 0):  # bissextile
            if day == 30:
                day = 1
                month += 1
        else:
            if day == 29:
                day = 1
                month += 1
    elif month in big_month:        #big month
        if day == 32:
            day = 1
            month += 1
    else:                           #small month
        if day == 31:
            day = 1
            month += 1
    if month == 13 :    
        month = 1
        year += 1
            
    return year,month,day,hour

def make_tryfile( year, month, day, hour ):  #make standard date
    Y = str( year )
    M = str( month )
    D = str( day )
    H = str( hour )
    if month < 10 :
        M = '0' + M
    if day < 10 :
        D = '0' + D
    if hour < 10 :
        H = '0' + H
    return Y+M+D+H

def recollect( year, month, day, hour, offset, filename ):   #try to read date from more new file after check_point time
    rotatetime = make_tryfile( year, month, day, hour )
    tryfile = filename + '.' + rotatetime
    brake = 0
    if os.path.exists( tryfile ):  
        recollect_file = open( tryfile, 'r' )
        recollect_file.seek( offset, 0 )
        recollect_last_position = offset
        while True:
            if brake > 1000:
                time.sleep( 0.1 )
                brake = 0
            line = recollect_file.readline()
            recollect_now_position = recollect_file.tell()
            if recollect_last_position == recollect_now_position:
                brake = 0
                recollect_file.close()
                break
            print line, 
            brake += 1
            recollect_last_position = recollect_now_position
    year,month,day,hour = new_day( year, month, day, hour )
    rotatetime = make_tryfile( year, month, day, hour )
    time.sleep(5)
    while rotatetime <= time.strftime( "%Y%m%d%H", time.localtime() ): 
        tryfile = filename + "." + rotatetime
        if not os.path.exists( tryfile ):
            year,month,day,hour = new_day( year, month, day, hour )
            rotatetime = make_tryfile( year, month, day, hour )
            continue
        recollect_file = open( tryfile, 'r' )
        recollect_last_position = 0
        time.sleep(5)
        while True:
            if brake > 1000:
                time.sleep( 0.1 )
                brake = 0
            line = recollect_file.readline()
            recollect_now_position = recollect_file.tell()
            if recollect_last_position == recollect_now_position:
                brake = 0
                recollect_file.close()
                break
            print line, 
            brake += 1
            recollect_last_position = recollect_now_position
        year,month,day,hour = new_day( year, month, day, hour )
        rotatetime = make_tryfile( year, month, day, hour )
            
def start( offset_file, filename ):
    if os.path.exists( offset_file ):
        File_offset = open( offset_file, 'r' )
        loads = File_offset.read()
        save_dict = pickle.loads( loads )
        offset = int(save_dict[ 'offset' ])
        year = int( save_dict[ 'year' ] )
        month = int( save_dict[ 'month' ] )
        day = int( save_dict[ 'day' ] )
        hour = int ( save_dict[ 'hour' ] )
        former_time = save_dict[ 'year'] + save_dict[ 'month' ] + save_dict[ 'day' ] + save_dict[ 'hour' ]
        if former_time  == time.strftime( "%Y%m%d%H", time.localtime() ):
   #         print "level:1"
            readfile( filename, offset, offset_file )    
        else:
  #          print "level:2"
            recollect( year, month, day, hour, offset, filename )
            readfile( filename, 0, offset_file  )    
    else:
 #       print "level:3"
        readfile( filename, 0, offset_file  )
    
    
                    
        
    
if  __name__ == '__main__':
    Largv = len( sys.argv )
    if Largv != 3 and Largv != 4:
        exit( -1 )
    if Largv == 3 :
        Offset_file = 'sourceCP.bip'
    else :
        Offset_file = sys.argv[3]
    Filename = sys.argv[ 1 ] 
    os.chdir( sys.argv[ 2 ] )
    pid = os.getpid()
    t = threading.Timer( 1, prevent_deadsubprocess, [pid] )
    t.start()
    start( Offset_file, Filename )






