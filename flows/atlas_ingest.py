import os, glob, datetime, time, shutil, httpx
from prefect import flow, task
from prefect.states import Failed
from prefect-sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
from astropy.time import Time

# connector = SqlAlchemyConnector(
#     connection_info=ConnectionComponents(
#         driver=SyncDriver.POSTGRESQL_PSYCOPG2,
#         username="coma",
#         password="ChangeMePlease",
#         host="localhost",
#         port=5433,
#         database="coma",
#     )
# )
#
# connector.save("coma-connector")

@task(log_prints=True)
def file_checker(basepath: str) -> list:
    full_path = os.path.join(basepath, "**/*.fits.fz")
    print(f"Poking for files in {full_path}")

    new_files=[]
    for path in glob.glob(full_path, recursive=True):
        if os.path.isfile(path):
            mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
            print("Found File {path} last modified: {mod_time}".format(path=path, mod_time=mod_time))
            new_files.append(path)   
    print("Files found:")
    print(len(new_files))
    return new_files

@task(log_prints=True)
def describe_fits(file: str) -> dict:
    api = "http://localhost:8001/api/v2/sci/fits/describe"
    json = { "fits_file": file }
    response = httpx.post(api, json=json,verify=False).json()
    job_id = response["id"]
    time.sleep(2)
    
    japi = f"http://localhost:8001/api/v2/sci/fits/describe/{job_id}"
    resp = httpx.get(japi, verify=False).json()
    data = resp["result"]["PARAMETERS"]
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

    api = "http://localhost:8001/api/v2/sci/fits/identify"
    response = httpx.post(api, json=json, verify=False).json()
    id = response['id']
    time.sleep(30)

    japi = f"http://localhost:8001/api/v2/fits/identify/{id}"
    resp = httpx.get(japi, verify=False).json()
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
        data["ISO-DATE-MID"] = datetime.datetime(1, 1, 1)
        data["ISO-DATE-MID"] = datetime.datetime(1,1,1)
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
    
    if data["INSTRUMENT"] == "no_instrument" or data["OBSERVATORY"] == "no_observatory" or data["MJD_MID"] == 0.0 or data["EXPTIME"] == 0.0 or data["FILTER"] == "no_filter":
        dead_letter(scratch_filepath)
        return Failed()
    else: 
        return data

@task(log_prints=True)
def get_pds4_lid(block_name: str, identity: str, ) -> list:
    with SqlAlchemyConnector.load(block_name) as connector:
        sql = f"select pds4_lid from objects where name = {identity}"
        row = connector.fetch_one(sql)
        print(row)
        return row

@task(log_prints=True)
def calibrate_fits(file: str) -> dict:
    api = "http://localhost:8001/api/v2/sci/sci/fits/calibrate"
    json = { "fist_file": file }
    response = httpx.post(api, json=json,verify=False).json()
    job_id = response["id"]
    
    japi = "http://localhost:8001/api/v2/sci/fits/calibrate/{job_id}".format(job_id=job_id)
    resp = httpx.get(japi, verify=False).json()
    time.sleep(3)
    data = resp["result"]["PARAMETERS"]
    return data

@task(log_prints=True)
def photometry_fits(file: str) -> dict:
    api = "http://localhost:8001/api/v2/sci/fits/photometry"
    apertures = [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 16.0, 20.0]
    # else:
    #     apertures = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 16.0, 20.0]

    json = { 
        "fits_file": file, 
        "apertures": apertures 
    }

    photom_resp = httpx.post(api, json=json, verify=False).json()
    id = photom_resp["id"]
    time.sleep(15)
    photometry = httpx.get(f"api/{id}", verify=False).json()
    return photometry

@task(log_prints=True)
def orbit_photometry(object:str)-> str:
    api = "http://localhost:8001/api/v2/sci/fits/orbit"
    print(object)
    return object 

@task(log_prints=True)
def move_to_datalake(scratch: str,data: dict):
    filename = os.path.basename(scratch)
    # Move file to directory path in datalake
    path = f"/data/staging/datalake/{ data['pds4_lid'] }/{ data['iso_date_lake'] }/{ data['instrument'] }/"
    shutil.move(scratch, path + filename)
    success_msg = f"Moved ATLAS { data['source_filepath'] } to datalake { path + filename } "
    print(success_msg)
    return success_msg 


@task(log_prints=True)
def database_inserts(image: dict, calibration: dict, photometry:dict, orbit: dict): 
    image_api = "http://localhost:8001/api/v2/images"
    calibration_api = "http://localhost:8001/api/v2/calibrations"
    photometry_api = "http://localhost:8001/api/v2/photometries"
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
    identity = identify_object(description)
    data = flight_checks(description, scratch)
    pds4_lid = get_pds4_lid("coma-connector", identity)
    if len(pds4_lid) == 0:
        dead_letter(scratch)
    calibration = calibrate_fits(scratch)
    photometry = photometry_fits(scratch)
    orbit = orbit_photometry(data["OBJECT"])
    if (calibration == None or photometry == None or orbit == None):
        dead_letter(scratch)
    database_inserts(data, calibration, photometry, orbit)
    move_to_datalake(scratch, data)

@flow(log_prints=True)
def dead_letter(file:str):
    print(f"The dead_letter file is: {file}")
    pass



# if __name__ == "__main__":
#     atlas_ingest.serve(name="atlas_ingest_deploy")

