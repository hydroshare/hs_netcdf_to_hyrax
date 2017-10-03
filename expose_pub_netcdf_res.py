# This script should run after iinit is executed with irods_environment set up to use 
# HydroShare iRODS proxy user home directory where HydroShare resources are stored.
# Functionality: check iRODS metadata isPublic and resourceType for each resource collection 
#                to copy resource over for only public netCDF resource types or netCDF file 
#                types in composite resource types from iRODS HydroShare zones via iget command 
#                and put the copied resource in a new directory /opt/inetcdf_public_hydroshare 
#                for THREDDS data server to point to so that only public netCDF resources or  
#                file types are cataloged for public viewing. It is also run nightly as a cron 
#                job to clean up files as needed, e.g., resources made from public to private 
#                are not cleaned up immediately for performance reasons, but cleaned up nightly 
#                by this cron job. In addition, the nightly run script will compare the modified
#                time stamp to make copies via iget as needed to make sure all copied files are 
#                latest versions 
#                
# Usage: can be run directly without parameter or with one parameter, resource uuid, that needs
#        to be made public from private, hence needs to be copyed over via iget if it is not 
#        already there or it is not the latest version based on modified time stamp to expose
#        the new public resource to hyrax server
# Author: Hong Yi

import subprocess
import sys
import os
import shutil

# These path variables need to be populated on a deployment server before running it
tgt_path = ''
data_zone_path = ''
user_zone_path = ''


def path_exist(path=''):
    if not path:
        return False

    if path:
        # enter to the absolute directory where all HydroShare federated zone resources are stored
        subprocess.check_call(['icd', path])

    proc = subprocess.Popen('ils', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode:
        return False
    else:
        return True


def get_netCDF_data_object_time_stamp(src_path):
    # src_path should be set in the form of /zone/home/proxyUser/res_id
    data_file_path = os.path.join(src_path, 'data', 'contents')
    proc = subprocess.Popen(['ils', '-rl', data_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode:
        raise Exception(proc.returncode, 'ils failed to list data file for the resource ' + src_path)
    else:
        # extract last modified time stamp for netCDF file
        line_list = stdout.splitlines()
        whole_path = ''
        for line in line_list:
            line = line.strip()
            strs = line.split()
            # the output from ils -l is in the format of 'user replica_number resource_name size last_modified_ts & filename' 
            if len(strs) == 1:
                # remove the last : in the output
                whole_path = line[:-1]
            elif len(strs) == 7 and strs[1] == '0':
                nc_fname = strs[6]
                if nc_fname.endswith('.nc'):
                    # netCDF file, need to check its last modified time stamp
                    nc_ts = strs[4]
                    # find relative path
                    if src_path.endswith('/'):
                        src_path = src_path[:-1]
                    ridx = src_path.rfind('/')
                    if ridx >= 0:
                        rid = src_path[ridx+1:]
                    else:
                        rid = src_path
                    idx = whole_path.find(rid)
                    if idx >= 0:
                        rel_path = whole_path[idx:]
                        return rel_path, nc_fname, nc_ts
                    else:
                        break

        # no netCDF file, return empty
        return '', '', ''
 

def copy_res(src_p):
    # src_p and tget_p have already been guaranteed to exist by calling routine
    rel_path, nc_fname, nc_ts = get_netCDF_data_object_time_stamp(src_p) 
    if rel_path and nc_fname and nc_ts:
        # only make copy and write time stamp where there is netCDF file included in the resource
        proc = subprocess.Popen(['iget', '-rf', src_p, tgt_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            raise Exception(proc.returncode, 'The resource ' + src_p + ' failed to copy to ' + tgt_path)
        else:
            file_path = os.path.join(tgt_path, rel_path)
            # write time stamp out for later comparison to check whether data refresh is needed
            tgt_nc_ts_fname = os.path.join(file_path, nc_fname+'.ts')
            with open(tgt_nc_ts_fname, 'w') as f:
                f.write(nc_ts)


def copy_to_target_as_needed(src_path):
    # src_path should be set in the form of /zone/home/proxyUser/res_id  
    # the calling routine only calls this method when the resource already exists in 
    # target path, so need to compare last modified time stamp to determine whether 
    # to refresh data or not
    rel_path, nc_fname, nc_ts = get_netCDF_data_object_time_stamp(src_path)
    if rel_path and nc_fname and nc_ts:
        tgt_p = os.path.join(tgt_path, rel_path)
        tgt_nc_ts_fname = os.path.join(tgt_p, nc_fname+'.ts')
        with open(tgt_nc_ts_fname, 'r') as f:
            tgt_ts = f.read() 
            if nc_ts != tgt_ts:
                # make the copy for data refresh
                copy_res(src_path, tgt_path)


def copy_src_to_tgt(rid):
    src1 = os.path.join(data_zone_path, rid)
    src2 = os.path.join(user_zone_path, rid)
    if path_exist(src1):
        src = src1
    elif path_exist(src2):
        src = src2

    tgt = os.path.join(tgt_path, rid)
    if not os.path.exists(tgt):
        copy_res(src)
    else:
        # need to compare last_modified time stamp to determine whether a data refresh copy is needed
        copy_to_target_as_needed(src)

 
def walk_all_resources(path=''):
    if path:
        # enter to the absolute directory where all HydroShare federated user zone resources are stored
        subprocess.check_call(['icd', path])
    else:
        # enter to the home directory where all HydroShare resources are stored
        subprocess.check_call(['icd'])

    proc = subprocess.Popen('ils', stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode:
        raise Exception(proc.returncode, stdout, stderr)
    else:
        line_list = stdout.splitlines()
        for line in line_list:
            line = line.strip()
            if line.startswith('C-') and not "bags" in line:
                strs = line.rsplit('/', 1)
                # print strs[1]
                ioutput = subprocess.check_output(["imeta", "ls", "-C", strs[1]])
                ioutline_list = ioutput.splitlines()
                meta_dict = {'isPublic': '',
                             'resourceType': '',
                             'bag_modified': ''}
                attr_str = ''
                for oline in ioutline_list:
                    if oline.startswith('attribute:'):
                        attr_strs = oline.split(':')
                        attr_str = attr_strs[1].strip()
                    elif oline.startswith('value:') and attr_str:
                        value_strs = oline.split(':')
                        meta_dict[attr_str] = value_strs[1].strip().lower()
                        attr_str = ''

                if meta_dict['resourceType'] == 'netcdfresource' or meta_dict['resourceType'] == 'compositeresource':
                    tgt = os.path.join(tgt_path, strs[1])
                    if meta_dict['isPublic'] == 'true':
                        # check whether this resource has already existed in tgt_path and only make copy when needed 
                        if not os.path.exists(tgt):
                            copy_res(strs[1])
                        else:
                             # need to compare last_modified time stamp to determine whether a data refresh copy is needed
                             copy_to_target_as_needed(strs[1])
                    else:
                        # check if a copy has already been made for this resource, and if yes, remove it as it is now made non-public
                        if os.path.exists(tgt):
                            shutil.rmtree(tgt)


# check whether the optional parameter is passed in
res_id = ''
if len(sys.argv) > 1:
    res_id = sys.argv[1].strip()

if res_id:
    # copy to target directory for hyrax server as needed for this just-made-public resource
    copy_src_to_tgt(res_id)
else:
    walk_all_resources()
    print 'start to process resources in user zone'
    walk_all_resources(user_zone_path)

