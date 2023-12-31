import os, glob, datetime, time, shutil, httpx
from prefect import flow, task
from prefect.states import Failed
from prefect_sqlalchemy import SqlAlchemyConnector
from astropy.time import Time
from datetime import datetime, timedelta

auth_header = {"Authorization": "Bearer bowg0k-qomq4P-8gazve-c0m3t5"}

# from prefect_sqlalchemy import ConnectionComponents, SyncDriver
# connector = SqlAlchemyConnector(
#     connection_info=ConnectionComponents(
#         driver=SyncDriver.POSTGRESQL_PSYCOPG2,
#         username="coma",
#         password="ChangeMePlease",
#         host="coma.ifa.hawaii.edu",
#         port=5433,
#         database="coma",
#     )
# )

# connector.save("coma-connector")

@task(log_prints=True)
def file_checker(basepath: str) -> list:
    full_path = os.path.join(basepath, "**/*.fits.fz")
    print(f"Poking for files in {full_path}")

    new_files=[]
    for path in glob.glob(full_path, recursive=True):
        if os.path.isfile(path):
            mod_time = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
            # print("Found File {path} last modified: {mod_time}".format(path=path, mod_time=mod_time))
            new_files.append(path)   
    print("Files found:")
    print(len(new_files))
    return new_files

@task(log_prints=True)
def describe_fits(file: str) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/describe"
    json = { "fits_file": file }
    response = httpx.post(api, json=json,verify=False).json()
    job_id = response["id"]
    time.sleep(2)
    
    japi = f"http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/describe/{job_id}"
    resp = httpx.get(japi, verify=False).json()
    data = resp["result"]
    # print("Describe result")
    # print(data)
    return data

@task(log_prints=True)
def copy_to_scratch(file: str) -> str:
    # Copy file to scratch dir for manipulating headers/calibration
    filename = os.path.basename(file)
    scratch_filepath = f"/data/staging/scratch/{filename}"
    shutil.copyfile(file, scratch_filepath)
    print(f"Copied ATLAS source {file} to scratch {scratch_filepath}")
    return scratch_filepath 

@task(log_prints=True)
def identify_object(description: dict) -> str:
    raw_object = description['ORIGINAL-OBJECT-RAW']
    mjd = float(description['MJD-MID'])
    ra = float(description['RA-J2000-APPROX'])
    dec = float(description['DEC-J2000-APPROX'])

    json = {"object": raw_object,
            "mjd": mjd,
            "ra": ra,
            "dec": dec}

    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/identify"
    response = httpx.post(api, json=json, verify=False).json()
    id = response['id']
    time.sleep(75)

    japi = f"http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/identify/{id}"
    resp = httpx.get(japi, verify=False).json()
    # print(f"The result of the comet identity is { resp['result'] }")
    return resp['result']

@task(log_prints=True)
def flight_checks(data: dict, scratch_filepath: str) -> dict:
    ###################################################################
    # Basic data integrity checks 
    ###################################################################
    try:
        data["OBSERVATORY"]
    except:
        print("No observatory key present - setting to no_observatory.") 
        data["OBSERVATORY"] = "no_observatory"   
    
    try:
        data["OBSCODE"]
    except:
        print("No obscode key present - setting to NULL.") 
        data["OBSCODE"] = "NULL"   

    try:
        data["INSTRUMENT"]
    except:
        print("No instrument key present - setting to no_instrument.") 
        data["INSTRUMENT"] = "no_instrument"   

    try:
        data["MJD-MID"]
    except:
        print("No mjd-mid key present - setting to 0.0.") 
        data["MJD-MID"] = 0.0   

    if data["MJD-MID"] == 0.0:
        data["ISO-DATE-MID"] = datetime(1, 1, 1)
    else:
        time_from_mjd = Time(data["MJD-MID"], format='mjd', scale='utc')
        data["ISO-DATE-MID"] =  time_from_mjd.to_value(format='isot')
        data["ISO-DATE-LAKE"] = time_from_mjd.to_value(format='iso', subfmt='date')
        data["ISO-UTC-START"] = datetime.strptime(data['ISO-DATE-MID'], '%Y-%m-%dT%H:%M:%S.%f')
        data["ISO-UTC-END"] = data["ISO-UTC-START"] + timedelta(minutes=1)

    try:
        data["EXPTIME"]
    except:
        print("No exposure time key present - setting to 0.0.") 
        data["EXPTIME"] = 0.0   
        
    try:
        data["FILTER"]
    except:
        print("No filter present - setting to no_filter.") 
        data["FILTER"] = "no_filter"

    match data["OBSTYPE"]:
        case "OBJECT":
            if data["IS-REDUCED"] == True:
                data["OBSTYPE"] = "reduced"
            else:
                data["OBSTYPE"] = "raw"
        case "BIAS":
            data["OBSTYPE"] = "bias"
        case "DARK":
            data["OBSTYPE"] = "dark"
        case "FLAT":
            data["OBSTYPE"] = "flat"
        case _:
            data["OBSTYPE"] = "no_type"
    
    
    if data["INSTRUMENT"] == "no_instrument" or data["OBSERVATORY"] == "no_observatory" or data["MJD-MID"] == 0.0 or data["EXPTIME"] == 0.0 or data["FILTER"] == "no_filter" or data["OBSTYPE"] == "no_type":
        dead_letter(scratch_filepath)
        return Failed()
    else: 
        return data

@task(log_prints=True)
def get_object_id(block_name: str, identity: str) -> str:
    print(f"The actual name of the object is: {identity}")
    with SqlAlchemyConnector.load(block_name) as connector:
        row = connector.fetch_one("SELECT id FROM objects WHERE name = :name LIMIT 1", parameters={"name": identity})
        print(f"Result returned by SQL for identity was {row}")
        return row

@task(log_prints=True)
def get_instrument_id(block_name: str, instrument: str) -> str:
    # print("The actual name of the instrument is:")
    # print(instrument)
    instrument = instrument.lower()
    with SqlAlchemyConnector.load(block_name) as connector:
        row = connector.fetch_one("SELECT id FROM instruments WHERE name = :instrument LIMIT 1", parameters={"instrument": instrument})
        # print(f"Result returned by SQL for instrument was {row}")
        return row

@task(log_prints=True)
def get_pds4_lid(block_name: str, identity: str) -> str:
    # print("The actual name of the object is:")
    # print(identity)
    with SqlAlchemyConnector.load(block_name) as connector:
        row = connector.fetch_one("SELECT pds4_lid FROM objects WHERE name = :name LIMIT 1", parameters={"name": identity})
        print(f"Result returned by SQL for identity for pds4_lid was {row}")
        return row

@task(log_prints=True)
def get_telescope_id(block_name: str, instrument: str) -> str:
    print(f"The actual instrument is {instrument}")
    instrument = instrument.lower()
    with SqlAlchemyConnector.load(block_name) as connector:
        row = connector.fetch_one("SELECT telescope_id FROM instruments WHERE name = :instrument LIMIT 1", parameters={"instrument": instrument})
        # print(f"Result returned by SQL was {row}")
        return row

@task(log_prints=True)
def get_filter_id(block_name: str, filter: str, telescope_id: str) -> str:
    print(f"The actual filter is {filter} and telescope_id is {telescope_id}")
    with SqlAlchemyConnector.load(block_name) as connector:
        row = connector.fetch_one("SELECT id FROM filters WHERE input_code = :filter AND telescope_id = :telescope_id LIMIT 1", parameters={"filter": filter, "telescope_id": telescope_id})
        # print(f"Result returned by SQL was {row}")
        return row

@task(log_prints=True)
def calibrate_fits(file: str) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/calibrate"
    json = { "fits_file": file }
    response = httpx.post(api, json=json,verify=False).json()
    job_id = response["id"]
    
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/calibrate/{job_id}".format(job_id=job_id)
    time.sleep(5)
    resp = httpx.get(japi, verify=False).json()
    data = resp["result"]
    # print(f"Calibrate result on file is {data}")
    return data

@task(log_prints=True)
def photometry_fits(file: str, object: str, phot_type: str) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/photometry"
    apertures = [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 16.0, 20.0]
    # else:
    #     apertures = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 16.0, 20.0]

    json = { 
        "fits_file": file,
        "object": object, 
        "apertures": apertures,
        "photometry_type": phot_type,
    }

    photom_resp = httpx.post(api, json=json, verify=False).json()
    job_id = photom_resp["id"]
    time.sleep(15)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/photometry/{job_id}".format(job_id=job_id)
    photometry = httpx.get(japi, verify=False).json()
    # print(f"Photometry result is { photometry }")
    return photometry

@task(log_prints=True)
def object_orbit(object: str)-> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/orbit"
    json = {
        "method": "jpl-horizons",
        "object": object
    }
    response = httpx.post(api, json=json, verify=False).json()
    job_id = response['id']
    time.sleep(5)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/orbit/{job_id}".format(job_id=job_id)
    resp = httpx.get(japi, verify=False).json()
    # print(resp)
    orbit = resp["result"]["ORBIT"]
    # print(orbit)
    return orbit

@task(log_prints=True)
def object_ephemerides(description: dict) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/ephem"
    
    description["ISO-UTC-START"] = description["ISO-UTC-START"].strftime('%Y-%m-%dT%H:%M:%S')
    description["ISO-UTC-END"] = description["ISO-UTC-END"].strftime('%Y-%m-%dT%H:%M:%S')

    json = {
        "object": description['OBJECT'],
        "dt-minutes": 2,
        "obscode": description["OBSCODE"],
        "utc-start": description["ISO-UTC-START"],
        "utc-end": description["ISO-UTC-END"]
    }
    # print("This is the eph json:")
    # print(json)
    response = httpx.post(api, json=json, verify=False).json()
    job_id = response['id']
    # print(f"The returned job for eph is {job_id}")
    time.sleep(30)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/ephem/{job_id}".format(job_id=job_id)
    resp = httpx.get(japi, verify=False).json()
    ephemerides = resp["result"]["PARAMETERS"]
    # print(ephemerides)
    return ephemerides


@task(log_prints=True)
def record_orbit(object: str, orbit:dict) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/coords"
    json = {
        "orbit": orbit,
        "object-name": object,
        "rhelio-max": 30.0,
        "dr-frac": 0.02
    }
    # print("This is the coord json:")
    # print(json)
    response = httpx.post(api, json=json, verify=False).json()
    job_id = response['id']
    # print(f"The returned job for orbit coords is {job_id}")
    time.sleep(30)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/coords/{job_id}".format(job_id=job_id)
    resp = httpx.get(japi, verify=False).json()
    orbit_coords = resp["result"]["PARAMETERS"]
    # print(orbit_coords)
    return orbit_coords


@task(log_prints=True)
def move_to_datalake(scratch: str,data: dict):
    filename = os.path.basename(scratch)
    # Move file to directory path in datalake
    path = f"/data/staging/datalake/{ data['PDS4-LID'] }/{ data['ISO-DATE-LAKE'] }/{ data['INSTRUMENT'] }/"
    if os.path.exists(path):
        shutil.move(scratch, path + filename)
    else:
        os.makedirs(path)
        shutil.move(scratch, path + filename)
    success_msg = f"Moved ATLAS { data['FITS-FILE'] } to datalake { path + filename } via scratch area."
    print(success_msg)
    return success_msg 


@task(log_prints=True)
def database_inserts(description: dict, calibration: dict, photometry:dict, ephemerides: dict, orbit_coords: dict): 
    image_api = "http://coma.ifa.hawaii.edu:8001/api/v2/images"
    cal_api = "http://coma.ifa.hawaii.edu:8001/api/v2/calibrations"
    phot_api = "http://coma.ifa.hawaii.edu:8001/api/v2/photometries"

    print(f"Inserting image record for {description['SOURCE-FILEPATH']}")
    image = {}
    image["object_id"] = description["OBJECT-ID"]
    image["filter_id"] = description["FILTER-ID"]
    image["instrument_id"] = description["INSTRUMENT-ID"]
    image["type"] = description["OBSTYPE"]
    image["mjd_mid"] = description["MJD-MID"]
    image["iso_date_mid"] = datetime.strptime(description['ISO-DATE-MID'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    image["exposure_time"] = description["EXPTIME"]
    image["gain"] = calibration["QUALITIES-INFO"]["GAIN"]
    image["pixel_scale"] = calibration["QUALITIES-INFO"]["PIXEL-SCALE"]
    image["source_filepath"] = description["SOURCE-FILEPATH"]
    image["lake_filepath"] = description["LAKE-FILEPATH"]
    # print("Image submitted")
    # print(image)
    image_resp = httpx.post(image_api, json=image, headers=auth_header, verify=False).json()
    # print(image_resp)
   
    print(f"Inserting calibration record for {description['SOURCE-FILEPATH']}")
    cal = {}
    cal["image_id"] = image_resp["image"]["ID"]
    cal["pixel_scale"] = calibration["QUALITIES-INFO"]["PIXEL-SCALE"]
    cal["wcs_n_stars"] = calibration["WCS-CALIB-INFO"]["WCS-NSTARS"]
    cal["wcs_rmsfit"] = calibration["WCS-CALIB-INFO"]["WCS-RMSFIT"]
    cal["wcs_catalog"] = calibration["WCS-CALIB-INFO"]["WCS-CATALOG"]
    cal["phot_n_stars"] = calibration["PHOT-CALIB-INFO"]["PHOT-NSTARS"]
    cal["phot_catalog"] = calibration["PHOT-CALIB-INFO"]["PHOT-CATALOG"]
    # cal["extinction"]
    # cal["extinction_err"]
    # cal["color_term"]
    # cal["color_term_err"]
    cal["zp_mag"] = calibration["PHOT-CALIB-INFO"]["ZPMAG"]
    cal["zp_mag_err"] = calibration["PHOT-CALIB-INFO"]["ZPMAGERR"]
    cal["zp_inst_mag"] = calibration["PHOT-CALIB-INFO"]["ZPINSTMAG"]
    cal["zp_inst_mag_err"] = calibration["PHOT-CALIB-INFO"]["ZPINSTMAGERR"]
    cal["psf_n_obj"] = calibration["QUALITIES-INFO"]["PSF-NOBJ"]
    cal["psf_fwhm_arcsec"] = calibration["QUALITIES-INFO"]["PSF-FWHM-ARCSEC"]
    cal["psf_major_axis_arcsec"] = calibration["QUALITIES-INFO"]["PSF-MAJOR-AXIS-ARCSEC"]
    cal["psf_minor_axis_arcsec"] = calibration["QUALITIES-INFO"]["PSF-MINOR-AXIS-ARCSEC"]
    cal["psf_pa_pix"] = calibration["QUALITIES-INFO"]["PSF-PA-PIX"]
    cal["limit_mag_5_sigma"] = calibration["QUALITIES-INFO"]["MAG-5-SIGMA"]
    cal["limit_mag_10_sigma"] = calibration["QUALITIES-INFO"]["MAG-10-SIGMA"]
    cal["n_density_mag_20"] = calibration["QUALITIES-INFO"]["NDENSITY-MAG-20"]
    cal["n_density_5_sigma"] = calibration["QUALITIES-INFO"]["NDENSITY-5-SIGMA"]
    cal["sky_backd_adu_pix"] = calibration["QUALITIES-INFO"]["SKY-BACKD-ADU-PIX"]
    cal["sky_backd_photons_pix"] = calibration["QUALITIES-INFO"]["SKY-BACKD-PHOTONS-PIX"]
    cal["sky_backd_adu_arcsec_2"] = calibration["QUALITIES-INFO"]["SKY-BACKD-ADU-ARCSEC2"]
    cal["sky_backd_photons_arcsec_2"] = calibration["QUALITIES-INFO"]["SKY-BACKD-PHOTONS-ARCSEC2"]
    cal["sky_backd_mag_arcsec_2"] = calibration["QUALITIES-INFO"]["SKY-BACKD-MAG-ARCSEC2"]
    
    cal_resp = httpx.post(cal_api, json=cal, headers=auth_header, verify=False).json()
    # print(cal_resp)

    print(f"Inserting photometry records for { photometry['result'][0]['method'] } for { description['SOURCE-FILEPATH'] }")
    for a in photometry["result"]:
        phot = a
        phot["calibration_id"] = cal_resp["calibration"]["ID"]
        phot["image_id"] = cal["image_id"]
        phot["object_id"] = image["object_id"]
        phot["sunvect_x"] = ephemerides["sunvect"]["x"][0]
        phot["sunvect_y"] = ephemerides["sunvect"]["y"][0]
        phot["sunvect_z"] = ephemerides["sunvect"]["z"][0]
        phot["heliocentric_au"] = ephemerides["eph"]["r"][0]
        phot["geocentric_au"] = ephemerides["eph"]["delta"][0]
        phot["phase_angle"] = ephemerides["eph"]["sto"][0]
        phot["true_anomaly"] = ephemerides["eph"]["trueanom"][0]
        phot["orbit_coords"] = { "orbit": orbit_coords }
        phot_resp = httpx.post(phot_api, json=phot, headers=auth_header, verify=False).json()
        print(phot_resp)
    
    success_msg = f"DB image, calibration, and photometry records inserted for FITS { description['SOURCE-FILEPATH'] }."
    print(success_msg)
    return success_msg 
    

@task(log_prints=True)
def packed_provisional_to_identity(packed_name: str) -> str:
    year_code = {'I': 1800, 'J': 1900, 'K': 2000}
    try:
        # check for numbered periodic comet or packed format
        if packed_name[0].isdigit():
            iau_comet_name = packed_name
        else:
            # Split packed provisional into components
            comet_type = packed_name[0]
            base_year = year_code[packed_name[1]]
            year = int(packed_name[2:4])
            halfmonth = packed_name[4]
            seq = int(packed_name[5:7])
            fragment = packed_name[7]

            # Calculate the IAU year
            iau_year = base_year + year
            
            # Format IAU comet name
            iau_comet_name = f"{comet_type}/{iau_year} {halfmonth}{seq}"
            if fragment != '0':
                iau_comet_name = iau_comet_name + '-' + fragment
        return iau_comet_name
    except ValueError:
        # Handle invalid input
        return "Invalid input format"

@flow(log_prints=True)
def atlas_ingest():
    atlas_path = "/data/staging/atlas/obj"
    files = file_checker(atlas_path)
    for file in files:
        sci_backend_processing(file)


@flow(log_prints=True)
def sci_backend_processing(file: str):
    scratch = copy_to_scratch(file)
    description = describe_fits(file)
    # time_from_mjd = Time(description["MJD-MID"], format='mjd', scale='utc')
    # description["ISO-DATE-MID"] =  time_from_mjd
    # description["ISO-DATE-LAKE"] = time_from_mjd.to_value(format='iso', subfmt='date')

    filepath = os.path.normpath(file).split(os.path.sep)
    if filepath[-4] == "atlas":
        identity = packed_provisional_to_identity(filepath[-2])
        description['OBJECT'] = identity
    else:
        identity = identify_object(description)

    description = flight_checks(description, scratch)

    description["OBJECT-ID"] = get_object_id("coma-connector", description["OBJECT"])
    if description["OBJECT-ID"] == None:
        dead_letter(scratch)
    else:
        description["OBJECT-ID"] = description["OBJECT-ID"][0]
    
    description["INSTRUMENT-ID"] = get_instrument_id("coma-connector", description["INSTRUMENT"])
    if description["INSTRUMENT-ID"] == None:
        dead_letter(scratch)
    else:
        description["INSTRUMENT-ID"] = description["INSTRUMENT-ID"][0]

    description["PDS4-LID"] = get_pds4_lid("coma-connector", identity)
    if description["PDS4-LID"] == None:
        dead_letter(scratch)
    else:
        description["PDS4-LID"] = description["PDS4-LID"][0]

    description["TELESCOPE-ID"] = get_telescope_id("coma-connector", description["INSTRUMENT"])
    if description["TELESCOPE-ID"] == None:
        dead_letter(scratch)
    else:
        description["TELESCOPE-ID"] = description["TELESCOPE-ID"][0]

    description["FILTER-ID"] = get_filter_id("coma-connector", description["FILTER"], description["TELESCOPE-ID"])
    if description["FILTER-ID"] == None:
        dead_letter(scratch)
    else:
        description["FILTER-ID"] = description["FILTER-ID"][0]

    description["SOURCE-FILEPATH"] = description["FITS-FILE"]
    path = f"/data/staging/datalake/{ description['PDS4-LID'] }/{ description['ISO-DATE-LAKE'] }/{ description['INSTRUMENT'] }/"
    description["LAKE-FILEPATH"] = path

    calibration = calibrate_fits(scratch)
    photometry_type = "APERTURE"
    photometry = photometry_fits(scratch, identity, photometry_type)
    orbit = object_orbit(description["OBJECT"])
    
    ephemerides = object_ephemerides(description)
    orbit_coords = record_orbit(description["OBJECT"], orbit)

    if (calibration == None or photometry == None or orbit == None or
            ephemerides == None):
        dead_letter(scratch)
    database_inserts(description, calibration, photometry, ephemerides, orbit_coords)
    move_to_datalake(scratch, description)

@flow(log_prints=True)
def dead_letter(file:str):
    print(f"The dead_letter file is: {file}")
    pass



# if __name__ == "__main__":
#     atlas_ingest.serve(name="atlas_ingest_deploy")

