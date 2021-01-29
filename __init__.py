"""
access GAVRT MySQL database

Databases
=========
The databases and their schemas are described in
http://gsc.lewiscenter.org/data_info/dss28_eac.php.

The server has these databases::

 'dss28_eac'
 'dss28_spec'
 'gavrt_sources'.


Database 'dss28_eac'
--------------------
has these tables::

  In [17]: dbplotter.get_public_tables()
  Out[17]: 
  (('angles',),     ('chan_cfg',),     ('conv_cfg',), ('fiber_cfg',),
   ('five_point',), ('pointing_cfg',), ('raster',),   ('raster_cfg',),
   ('rf_cfg',),     ('rss_cfg',),      ('seti_cfg',), ('seti_frame',),
   ('tlog',),       ('weather',),      ('xpwr',),     ('xpwr_cfg',),
   ('xscan',),      ('zplot',),        ('zplot_cfg',))

Database 'gavrt_sources'
------------------------
has these tables::

 'catalog',
 'class',
 'source'

Table columns
-------------
'angles' columns::

  angles_id,
  year, doy, utc, epoch, az, el, status
  
'catalog' columns::

  catalog_id, name
  
'chan_cfg' columns::

  chan_cfg_id,
  year, doy, utc, epoch, chan, center_freq, tdiode
  
'class' columns::

  class_id, name, description
  
'conv_cfg' columns::

  conv_cfg_id,
  year, doy, utc, epoch, converter, mode_a, ifbw_a, bbbw_a, atten_a,
                                    mode_b, ifbw_b, bbbw_b, atten_b, lock_status
  
'five_point' columns::

  five_point_id,
  xpwr_cfg_id, year, doy, utc, epoch, source_id, chan, tsrc, az, el, ha, dec, 
  xdec_off, dec_off
  
'pointing_cfg' columns::

  pointing_cfg_id,
  year, doy, utc, epoch, man, plx, semod, refrctn, delut, model
  
'raster' columns::

  raster_id,
  raster_cfg_id, year, doy, utc, epoch, xdecoff, decoff, ha, dec, tsrc
  
'raster_cfg' columns::

  raster_cfg_id,
  rss_cfg_id, year, doy, utc, epoch, source_id, chan, freq, rate, step
  
'rf_cfg' columns::

  rf_cfg_id,
  year, doy, utc, epoch, feed, diodex, diodey, pol, transfer
  
'rss_cfg' columns::

  rss_cfg_id,
  year, doy, utc, chan, sky_freq, feed, pol, nd, if_mode, if_bw, bb_bw, fiber_chan
  
'source' columns::

  source_id, catalog_id, class_id,
  name, RA, Dec, size_dec, size_xdec, reference, aka
  
'tlog' columns::

  tlog_id,
  rss_cfg_id, year, doy, utc, epoch, chan, top, integ, az, el, diode, level, cryo
  
'weather' columns::

  weather_id,
  datetime, pressure, temp, humidity, wind_speed, wind_dir
  
'xpwr' columns::

  xpwr_id,
  xpwr_cfg_id, year, doy, utc, epoch, tsys, az, el, ha, dec, offset
  
'xpwr_cfg' columns::

  xpwr_cfg_id, 
  rss_cfg_id, source_id, cal_src_id, year, doy, utc, epoch, axis, chan, cal_flux
  
'xscan' columns::

  xscan_id,
  xpwr_cfg_id, year, doy, utc, epoch, tsrc, stdev, bl_stdev, az, az_offset, el,
  el_offset, ha, dec, offset, bw, corr
"""
import logging
import MySQLdb
import os
import pickle

from support import mysql

logger = logging.getLogger(__name__)

_host,_user,_pw = pickle.load(open(os.environ['HOME']+"/.GAVRTlogin.p", "rb" ))

class DSS28db(mysql.BaseDB):
  """
  subclass for the DSS-28 EAC database
  
  provides methods for handling tables
  
  Attributes::
    logger   - logging.Logger object
    receiver - receivers which provide data
    sessions - dict of sessions obtained with 'get_session'
  """
  last_id = {'chan_cfg': None, 
             'conv_cfg': None,
             'raster_cfg': None, 
             'tlog': None}
             
  def __init__(self, host=_host, user=_user, pw=_pw,
                     name='dss28_eac', port=3306):
    """
    create an instance BaseDB subclass for the DSS-28 EAC database
    
    The defaults for BaseDB are for the DSS-28 EAC database
    """
    mylogger = logging.getLogger(logger.name+".DSS28db")
    super(DSS28db, self).__init__(host=host, user=user, pw=pw, name=name,
                                       port=port)
    self.logger = mylogger
    self.logger.debug("__init__: get last table IDs")
    # ID of last record read
    for table in self.last_id.keys():
      try:
        self.last_id[table] = super().getLastID(table)
      except AttributeError as exception:
        self.logger.error("__init__: could not get ID; %s", str(exception))
    self.sessions = {}

  def insertRecord(self, table, rec):
    """
    not allowed for subclass
    """
    self.logger.warning("insertRecord: not allowed for %s", self.name)

  def updateValues(self, vald, table): 
    """
    not allowed for subclass
    """
    self.logger.warning("updateValues: not allowed for %s", self.name)
        
  def extract_boresight_data(self, year, doy):
    """
    Get the metadata for the boresights on the designated day.

    The boresights are extracted from table 'xscan'.  Missing 'el' data are
    obtained from table 'xpwr'.  The source, scan axis and channel are obtained
    from table 'xpwr_cfg'.  The receiver data are obtained from table 'rss_cfg'.

    Returns a dictionary like this::
    
      {'utc':        list of datetime.timedelta,
       'epoch':      list of float,
       'az':         list of float,
       'el':         list of value,
       'chan':       list of int,
       'tsrc':       list of float,
       'axis':       list of str,
       'source':     list of str,
       'xpwr_cfg_id: list of int',
       'xscan_id':   list of int,
       'source_id':  list of int,
       'rx':         list of dict}
     
    An 'rx' dict looks like this::
    
      { 2: {'if_bw':    float,
            'if_mode':  str,
            'pol':      str,
            'sky_freq': float,
            'utc':      datetime.timedelta},
        4: { ... },
       ....
       16: { ... }}

    @param year : year of observation
    @type  year : int

    @param doy : day of year
    @type  doy : int

    @return: dict
    """
    # Get the boresight data from xscan
    columns = "utc, epoch, tsrc, az, el, xscan_id, xpwr_cfg_id"
    boresight_data = self.get_rows_by_date("xscan", columns, year, doy)

    # Get the missing elevation data from xpwr
    times = boresight_data['utc']
    power_data = self.get_rows_by_time('xpwr',['utc','el','tsys'],
                                     year,doy,times)
    # Fix the missing elevation data
    boresight_data['el'] = power_data['el']

    # Get the source information from gavrt_sources.source
    columns = "source_id, axis, chan"
    for column in columns.split(','):
      boresight_data[column.strip()] = []
    for cfg_id in boresight_data['xpwr_cfg_id']:
      response = self.get_as_dict("select "
                        + columns
                        + " from xpwr_cfg where xpwr_cfg_id="+str(cfg_id)+";")
    for key in list(response.keys()):
      boresight_data[key].append(response[key][0])
    boresight_data['source'] = []
    for source_id in boresight_data['source_id']:
      response = self.get_as_dict("select name from gavrt_sources.source where source_id="
                        +str(source_id)+";")
      boresight_data['source'].append(response['name'][0])

    # Get the receiver information from rss_cfg
    columns = "utc,sky_freq,pol,if_mode,if_bw"
    boresight_data['rx'] = []
    for time in times:
      boresight_data['rx'].append(self.get_receiver_data(year,doy,time,columns))

    return boresight_data

  def get_receiver_data(self, year=None, doy=None, time=None, epoch=None,
                              columns=[]):
    """
    Get the receiver state at a given time

    This creates a dictionary keyed with channel number and returns a dictionary
    of the receiver configuration, keyed with specified in the columns, that was
    in effect at the given time.

    Notes
    =====

    The challenge here is to get the latest configuration data for each channel
    at or prior to the specified time.  That channel may have been configured on
    the same day or a prior day. The method we'll use is to find the ID of last
    configuration change and assume that the IDs are sequential in date/time.

    @param db : database
    @type  db : Mysql.BaseDB instance

    @param year : year of observation
    @type  year : int

    @param doy : day of year
    @type  doy : int

    @param time : UTC for the requested receiver state
    @type  time : datetime.timedelta

    @param columns : data items to be returned
    @type  columns : list of str

    @return: dict
    """
    if year and doy and utc:
      pass
    elif epoch:
      pass
    else:
      self.logger.error("get_receiver_data: time spec required")
      raise RuntimeError("No time specification")
    # get the last record at or before year/doy/utc
    latest_data = self.get_as_dict("select rss_cfg_id,year,doy,utc from rss_cfg"
                        +" where year <= "+str(year)
                        +" and doy <= "+str(doy)
                        +" and utc <= '"+str(time)
                        +"' order by year desc, doy desc, utc desc limit 1;")
    cfg_ID = latest_data['rss_cfg_id'][0]
    self.receiver = {}
    columns = columns.replace(" ","")
    column_keys = columns.split(',')
    for key in column_keys:
      self.receiver[key] = {}
      for chan in [2,4,6,8,10,12,14,16]:
        rx_data = self.get_as_dict("select "+columns
                       +" from rss_cfg where rss_cfg_id <= "+str(cfg_ID)
                       +" and chan = "+str(chan)
                       +" order by rss_cfg_id desc limit 1;")
        
        index = column_keys.index(key)
        self.receiver[key][chan] = rx_data[key][0]
    return self.receiver

  def get_Tsys(self, chan, start, stop):
    """
    Get system temperatures from tlog
    
    @param start : UNIXtime at start of selection
    @type  start : float
    
    @param stop : UNIXtime at end of selection
    @type  stop :float
    """
    query = \
        'select epoch, top from tlog where chan = %d and epoch >= %f and epoch <= %f' \
                                                                % (chan, start, stop)
    try:
      response = self.get_as_dict(query)
      return response
    except Exception as details:
      self.logger.error("get_Tsys: error: %s", str(details))
      return None
 
  def get_session(self, year, doy):
    """
    get IDs for an observing session
    """
    if (year in self.sessions) == False:
      self.sessions[year] = {}
    self.sessions[year][doy] = Session(self, year, doy)
    return self.sessions[year][doy]

  def get_source_names(self, source_IDs):
    """
    Get the source information from gavrt_sources.source
    
    Returns a dict with source names for the source IDs provided
    """
    names = {'source': []}
    self.logger.debug("get_source_names: for %s", source_IDs)
    for source_id in source_IDs:
      if source_id:  # no source_id = 0
        response = self.get_as_dict(
                       "select name from gavrt_sources.source where source_id="
                       +str(source_id)+";")
        names['source'].append(response['name'][0])
      else:
        names['source'].append([None])
    return names

