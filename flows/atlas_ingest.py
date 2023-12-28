import os, glob, datetime, time, shutil, httpx
from prefect import flow, task
from prefect.states import Failed
from prefect_sqlalchemy import SqlAlchemyConnector
from astropy.time import Time
from datetime import datetime, timedelta

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
            print("Found File {path} last modified: {mod_time}".format(path=path, mod_time=mod_time))
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
    print("Describe result")
    print(data)
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
    print(f"The result of the comet identity is { resp['result'] }")
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
        data["ISO-DATE-MID"] = datetime(1,1,1)
    else:
        time_from_mjd = Time(data["MJD-MID"], format='mjd', scale='utc')
        data["ISO-DATE-MID"] =  time_from_mjd.to_value(format='iso', subfmt='date_hms')
        data["ISO-DATE-LAKE"] = time_from_mjd.to_value(format='iso', subfmt='date')

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
    
    if data["INSTRUMENT"] == "no_instrument" or data["OBSERVATORY"] == "no_observatory" or data["MJD-MID"] == 0.0 or data["EXPTIME"] == 0.0 or data["FILTER"] == "no_filter":
        dead_letter(scratch_filepath)
        return Failed()
    else: 
        return data

@task(log_prints=True)
def get_pds4_lid(block_name: str, identity: str, ) -> str:
    with SqlAlchemyConnector.load(block_name) as connector:
        row = connector.fetch_one("SELECT pds4_lid FROM objects WHERE name = :name", parameters={"name": identity})
        print(f"Result returned by SQL was {row}")
        return row[0]

@task(log_prints=True)
def calibrate_fits(file: str) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/calibrate"
    json = { "FITS-FILE": file }
    response = httpx.post(api, json=json,verify=False).json()
    job_id = response["id"]
    
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/calibrate/{job_id}".format(job_id=job_id)
    time.sleep(5)
    resp = httpx.get(japi, verify=False).json()
    data = resp["result"]
    print(f"Calibrate result on file is {data}")
    return data

@task(log_prints=True)
def photometry_fits(file: str, object: str, phot_type: str) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/photometry"
    apertures = [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 16.0, 20.0]
    # else:
    #     apertures = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 16.0, 20.0]

    json = { 
        "FITS-FILE": file,
        "OBJECT-NAME": object, 
        "APERTURES": apertures,
        "PHOTOMETRY-TYPE": phot_type,
    }

    photom_resp = httpx.post(api, json=json, verify=False).json()
    job_id = photom_resp["id"]
    time.sleep(30)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/fits/photometry/{job_id}".format(job_id=job_id)
    photometry = httpx.get(japi, verify=False).json()
    print(f"Photometry result is { photometry }")
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
    print(resp)
    orbit = resp["result"]["ORBIT"]
    print(orbit)
    return orbit

@task(log_prints=True)
def object_ephemerides(description: dict) -> dict:
    api = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/ephem"
    json = {
        "object": description['OBJECT'],
        "dt-minutes": 2,
        "obscode": description["OBSCODE"],
        "utc-start": description["ISO-UTC-START"].strftime('%Y-%m-%dT%H:%M:%S'),
        "utc-end": description["ISO-UTC-END"].strftime('%Y-%m-%dT%H:%M:%S')
    }
    print("This is the eph json:")
    print(json)
    response = httpx.post(api, json=json, verify=False).json()
    job_id = response['id']
    print(f"The returned job for eph is {job_id}")
    time.sleep(30)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/ephem/{job_id}".format(job_id=job_id)
    resp = httpx.get(japi, verify=False).json()
    ephemerides = resp["result"]
    print(ephemerides)
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
    print("This is the coord json:")
    print(json)
    response = httpx.post(api, json=json, verify=False).json()
    job_id = response['id']
    print(f"The returned job for orbit coords is {job_id}")
    time.sleep(30)
    japi = "http://coma.ifa.hawaii.edu:8001/api/v2/sci/comet/coords/{job_id}".format(job_id=job_id)
    resp = httpx.get(japi, verify=False).json()
    orbit_coords = resp["result"]
    print(orbit_coords)
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
def database_inserts(image: dict, calibration: dict, photometry:dict, orbit: dict, orbit_coords: dict): 
    image_api = "http://coma.ifa.hawaii.edu:8001/api/v2/images"
    calibration_api = "http://coma.ifa.hawaii.edu:8001/api/v2/calibrations"
    photometry_api = "http://coma.ifa.hawaii.edu:8001/api/v2/photometries"
    pass
    # image_resp = httpx.post(image_api, json=json, verify=False).json()
    # 
    # calibration["image_id"]= image["id"]
    # 
    # calibration_json = {"image_id": image["id"], "pixel_scale": image["pixel_scale"],
    #     "wcs_nstars": calibraton["wcs_nstars"], "wcs_rmsfit": calibration["wcs_rmsfit"], 
    #     "wcs_catalog": calibration["wcs_catalog"], "phot_nstars": phot_nstars, 
    #     "phot_catalog": phot_catalog, 
    #     "zp_mag": zp_mag, "zp_mag_err": zp_mag_err, 
    #     "zp_inst_mag": zp_inst_mag, "zp_inst_mag_err": zp_inst_mag_err,
    #     "psf_nobj": psf_nobj, "psf_fwhm_arcsec": psf_fwhm_arcsec, "psf_major_axis_arcsec": psf_major_axis_arcsec,    "psf_minor_axis_arcsec": psf_minor_axis_arcsec, "psf_pa_pix": psf_pa_pix, "psf_pa_world": psf_pa_world,
    #     "limit_mag_5_sigma": limit_mag_5_sigma, "limit_mag_10_sigma": limit_mag_10_sigma,
    #     "ndensity_mag_20": ndensity_mag_20, "ndensity_5_sigma": ndensity_5_sigma,
    #     "sky_backd_adu_pix": sky_backd_adu_pix, "sky_backd_photons_pix": sky_backd_photons_pix,
    #     "sky_backd_adu_arcsec2": sky_backd_adu_arcsec2, "sky_backd_photons_arcsec2": sky_backd_photons_arcsec2,
    #     "sky_backd_mag_arcsec2": sky_backd_mag_arcsec2, "calibration_filepath": scratch_filepath
    # }
    #
    # calibrate_resp = httpx.post(calibration_api, json=json, verify=False).json()
    # 
    # 
    # 
    # photometry_resp = httpx.post(photometry_api, json=json, verify=False).json()

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
    time_from_mjd = Time(description["MJD-MID"], format='mjd', scale='utc')
    description["ISO-DATE-MID"] =  time_from_mjd.to_value(format='iso', subfmt='date_hms')
    description["ISO-DATE-LAKE"] = time_from_mjd.to_value(format='iso', subfmt='date')

    filepath = os.path.normpath(file).split(os.path.sep)
    if filepath[-4] == "atlas":
        identity = packed_provisional_to_identity(filepath[-2])
        description['OBJECT'] = identity
    else:
        identity = identify_object(description)

    description = flight_checks(description, scratch)
    pds4_lid = get_pds4_lid("coma-connector", identity)
    if pds4_lid == None:
        dead_letter(scratch)
    else:
        description['PDS4-LID'] = pds4_lid
    
    calibration = calibrate_fits(scratch)
    photometry_type = "APERTURE"
    photometry = photometry_fits(scratch, identity, photometry_type)
    orbit = object_orbit(description["OBJECT"])
    description["ISO-UTC-START"] = datetime.strptime(description['ISO-DATE-MID'], '%Y-%m-%d %H:%M:%S.%f')

    description["ISO-UTC-END"] = description["ISO-UTC-START"] + timedelta(minutes=1)
    ephemerides = object_ephemerides(description)
    orbit_coords = record_orbit(description["OBJECT"], orbit)

    if (calibration == None or photometry == None or orbit == None or
            ephemerides == None):
        dead_letter(scratch)
    database_inserts(description, calibration, photometry, orbit, orbit_coords)
    move_to_datalake(scratch, description)

@flow(log_prints=True)
def dead_letter(file:str):
    print(f"The dead_letter file is: {file}")
    pass



# if __name__ == "__main__":
#     atlas_ingest.serve(name="atlas_ingest_deploy")

