import gzip
import os
import sys
import json
import logging

logger = logging.getLogger('dcnet.dcnet')

def procLogHandler(handler, loggerDets):
    
    if( handler is None ):
        return
        
    if( 'level' in loggerDets ):
        handler.setLevel( loggerDets['level'] )    
        
        if( loggerDets['level'] == logging.ERROR ):
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s :\n%(message)s')
            handler.setFormatter(formatter)

    if( 'format' in loggerDets ):
        
        loggerDets['format'] = loggerDets['format'].strip()
        if( loggerDets['format'] != '' ):
            formatter = logging.Formatter( loggerDets['format'] )
            handler.setFormatter(formatter)

    logger.addHandler(handler)

def setLoggerDets(logger, loggerDets):

    if( len(loggerDets) == 0 ):
        return

    consoleHandler = logging.StreamHandler()

    if( 'level' in loggerDets ):
        logger.setLevel( loggerDets['level'] )
    else:
        logger.setLevel( logging.INFO )

    if( 'file' in loggerDets ):
        loggerDets['file'] = loggerDets['file'].strip()
        
        if( loggerDets['file'] != '' ):
            fileHandler = logging.FileHandler( loggerDets['file'] )
            procLogHandler(fileHandler, loggerDets)

    procLogHandler(consoleHandler, loggerDets)

def setLogDefaults(params):
    
    params['log_dets'] = {}

    if( params['log_level'] == '' ):
        params['log_dets']['level'] = logging.INFO
    else:
        
        logLevels = {
            'CRITICAL': 50,
            'ERROR': 40,
            'WARNING': 30,
            'INFO': 20,
            'DEBUG': 10,
            'NOTSET': 0
        }

        params['log_level'] = params['log_level'].strip().upper()

        if( params['log_level'] in logLevels ):
            params['log_dets']['level'] = logLevels[ params['log_level'] ]
        else:
            params['log_dets']['level'] = logging.INFO
    
    params['log_format'] = params['log_format'].strip()
    params['log_file'] = params['log_file'].strip()

    if( params['log_format'] != '' ):
        params['log_dets']['format'] = params['log_format']

    if( params['log_file'] != '' ):
        params['log_dets']['file'] = params['log_file']


def genericErrorInfo(slug=''):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    
    errMsg = fname + ', ' + str(exc_tb.tb_lineno)  + ', ' + str(sys.exc_info())
    logger.error(errMsg + slug)

    return errMsg


def readTextFromFile(infilename):

    text = ''

    try:
        with open(infilename, 'r') as infile:
            text = infile.read()
    except:
        print('\treadTextFromFile()error filename:', infilename)
        genericErrorInfo()
    

    return text

def getDictFromFile(filename):

    try:

        if( os.path.exists(filename) == False ):
            return {}

        return getDictFromJson( readTextFromFile(filename) )
    except:
        print('\tgetDictFromFile(): error filename', filename)
        genericErrorInfo()

    return {}

def getDictFromJson(jsonStr):

    try:
        return json.loads(jsonStr)
    except:
        genericErrorInfo()

    return {}

def dumpJsonToFile(outfilename, dictToWrite, indentFlag=True, extraParams=None):

    if( extraParams is None ):
        extraParams = {}

    extraParams.setdefault('verbose', True)

    try:
        outfile = open(outfilename, 'w')
        
        if( indentFlag ):
            json.dump(dictToWrite, outfile, ensure_ascii=False, indent=4)#by default, ensure_ascii=True, and this will cause  all non-ASCII characters in the output are escaped with \uXXXX sequences, and the result is a str instance consisting of ASCII characters only. Since in python 3 all strings are unicode by default, forcing ascii is unecessary
        else:
            json.dump(dictToWrite, outfile, ensure_ascii=False)

        outfile.close()

        if( extraParams['verbose'] ):
            logger.info('\tdumpJsonToFile(), wrote: ' + outfilename)
    except:
        genericErrorInfo('\n\terror: outfilename: ' + outfilename)
        return False

    return True

def getTextFromGZ(path):
    
    path = path.strip()
    if( path == '' ):
        return ''
        
    try:
        infile = gzip.open(path, 'rb')
        txt = infile.read().decode('utf-8')
        infile.close()

        return txt
    except:
        genericErrorInfo(f'Error path: "{path}"')

    return ''

def getDictFromJsonGZ(path):

    json = getTextFromGZ(path)
    if( len(json) == 0 ):
        return {}
    return getDictFromJson(json)

def gzipTextFile(path, txt):
    
    try:
        with gzip.open(path, 'wb') as f:
            f.write(txt.encode())
    except:
        genericErrorInfo()

def calc_homogeneity(unique_count, total):

    if( unique_count == 1 ):
        return 1
    
    if( total == 0 ):
        return -1

    return 1 - (unique_count/total)

def get_mov_imdb_id(mov_link, split_key='/title/'):
    #movie link example: https://www.imdb.com/title/tt20218618/?ref_=nm_flmg_dr_1
    #director link: https://www.imdb.com/name/nm0009190/
    return mov_link.split(split_key)[-1].split('/')[0]