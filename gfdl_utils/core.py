import xarray as xr
import shutil
import glob
import os
import getpass
import time as time_module

def open_frompp(
    pp,
    ppname,
    out,
    local,
    time,
    add,
    dmget=False,
    mirror=False,
    prefix=f"/vftmp/{getpass.getuser()}",
    **kwargs
    ):
    """
    
    Open to a dataset from archive based on details of
    postprocess path
    
    Parameters
    ----------
    pp : str
        Path to postprocess directory
    ppname : str
        Name of postrocess file
    out : str
        Averaging of postprocess (ts or av)
    local : str
        Details of local file structure
        Commonly e.g. annual/5yr or annual_5yr
    time : str
        Time string
    add : str or list of str
        Additional string in filename
        
        If `add='*'`, all available matching variables are opened.
        
    dmget : Bool (default=False)
        If True, issues dmget command and waits until data has all migrated
        to disk before attempting to open it with xarray.
    mirror : Bool (default=False)
    prefix : str
    **kwargs :
        Any other keyword arguments are passed directly to
        xarray.open_mfdataset

    Returns
    -------
    ds : xarray.Dataset
        
    """

    if dmget and mirror:
        raise ValueError("Can not set both `dmget=True` and `mirror=True`.")

    # -------------------------------------------------
    # Discover variables
    # -------------------------------------------------

    def _discover_available_vars():

        allvars = get_varnames(pp, ppname)

        if allvars is None:
            return []

        matched = []

        for var in allvars:
            paths = glob.glob(
                get_pathspp(pp, ppname, out, local, time, var)
            )

            if len(paths) > 0:
                matched.append(var)

        return sorted(matched)

    if add == "*":
        add = _discover_available_vars()

        if len(add) == 0:
            raise FileNotFoundError(
                f"No files found for wildcard add='*' "
                f"with time pattern '{time}'."
            )

    if isinstance(add, str):
        add = [add]

    if not isinstance(add, list):
        raise TypeError("`add` must be a string or list of strings.")

    # -------------------------------------------------
    # Collect paths for all variables first
    # -------------------------------------------------

    paths_by_var = {}
    all_paths = []

    for var in add:

        paths = sorted(
            glob.glob(
                get_pathspp(pp, ppname, out, local, time, var)
            )
        )

        if len(paths) == 0:
            raise FileNotFoundError(
                f"No files found for variable '{var}' "
                f"with time pattern '{time}'."
            )

        paths_by_var[var] = paths
        all_paths.extend(paths)

    # -------------------------------------------------
    # Single dmget call
    # -------------------------------------------------

    if dmget:

        var_string = ", ".join(add)

        print(
            f"Issuing dmget for variables: {var_string}.",
            end=" "
        )

        issue_dmget(all_paths)

        while not query_all_ondisk(all_paths):
            time_module.sleep(0.1)

        print("Migration complete.")

    elif mirror:

        print(f"Mirroring paths at '{prefix}'.", end=" ")

        mirror_path(all_paths, prefix=prefix)

        for var in add:
            paths_by_var[var] = [
                f"{prefix}{p}"
                for p in paths_by_var[var]
            ]

        print("Mirroring complete.")

    # -------------------------------------------------
    # Open datasets
    # -------------------------------------------------

    datasets = []

    for var in add:

        open_kwargs = dict(kwargs)

        open_kwargs["combine"] = "nested"
        open_kwargs["concat_dim"] = "time"
        open_kwargs["coords"] = "minimal"
        open_kwargs["data_vars"] = "minimal"
        open_kwargs["compat"] = "override"
        open_kwargs.setdefault("join", "outer")

        ds_var = xr.open_mfdataset(
            paths_by_var[var],
            use_cftime=True,
            **open_kwargs,
        )

        datasets.append(ds_var)

    if len(datasets) == 1:
        return datasets[0]

    return xr.merge(
        datasets,
        compat="override",
        join="outer",
    )

def get_pathspp(pp,ppname,out,local,time,add):
    """
    Create a full path based on details of postprocess path
    
    Parameters
    ----------
    pp : str
        Path to postprocess directory
    ppname : str
        Name of postrocess file
    out : str
        Averaging of postprocess (ts or av)
    local : str
        Details of local file structure
        Commonly e.g. annual/5yr or annual_5yr
    time : str
        Time string
    add : str
        Additional string in filename
        If `out` is ts, this would be the variable name
        If `out` is av, this could be 'ann' (for annual data)
            or a number corresponding to the month 
            (for monthly climatology)
        
    Returns
    -------
    path : str
        Path including wildcards
    paths : list, str
        List of strings corresponding to expanded wildcards
        
    """
    filename = ".".join([ppname,time,add,'nc'])
    path = "/".join([pp,ppname,out,local,filename])
    return path.replace("//", "/")

def get_pathstatic(pp,ppname):
    """
    
    Get the path to the static grid file associated with
    particular postprocessed data.
    
    Parameters
    ----------
    pp : str
        Path to postprocess directory
    ppname : str
        Name of postrocess file
    
    Returns
    -------
    path : str
        Path to static grid
        
    """
    static = ".".join([ppname,'static','nc'])
    path = "/".join([pp,ppname,static])
    return path

def open_static(pp,ppname,dmget=False):
    """
    
    Get the path to the static grid file associated with
    particular postprocessed data.
    
    Parameters
    ----------
    pp : str
        Path to postprocess directory
    ppname : str
        Name of postrocess file
    
    Returns
    -------
    ds : xarray.Dataset
        Static grid file dataset
        
    """
    ds_path = get_pathstatic(pp,ppname)
    if dmget:
        print("Issuing dmget command to migrate data to disk.", end=" ")
        issue_dmget([ds_path])
        while not(query_all_ondisk([ds_path])):
            time_module.sleep(0.1)
        print("Migration complete.")
    return xr.open_dataset(ds_path)

def issue_dmget(path):
    """
    Issue a dmget command to the system for the specified path
    """
    if type(path)==list:
        cmd = f"dmget {' '.join(path)} &"
    elif type(path)==str:
        cmd = f"dmget {path} &"
    out = os.system(cmd)
    if out != 0:
        print(f"Warning: dmget launch returned nonzero exit code {out} for command: {cmd}")
    return out

def query_dmget(user=getpass.getuser(), out=False):
    """
    Check `dmwho` output for username. Returns 1 when user still in the queue and 0 if queue is `clean`.
    Option `out` prints output of command if not empty
    """
    cmd = f'dmwho | grep {user}'
    output = os.popen(cmd).read()
    if len(output) == 0:
        return 0
    else:
        if out:
            print(output)
        return 1
    
def query_ondisk(path):
    """
    Determine whether the files associated with [path] have been migrated from tape onto disk.
    Returns a dictionary with keys-value pairs for the path and a boolean: True for disk, False for not.
    """
    cmd = f"dmls -l {path}"
    outputs = os.popen(cmd).read().split('\n')
    ondisk = {}
    for output in outputs:
        if output.strip() == "":
            continue
        if ('(REG)' in output) or ('(DUL)' in output):
            ondisk[output.split(' ')[-1]]=True
        else:
            ondisk[output.split(' ')[-1]]=False
    return ondisk

def query_all_ondisk(paths):
    """
    Determine whether all of the files in [paths], assumed to be a list of lists of paths,
    have been migrated from tape onto disk. Use `query_ondisk` for more granular queries.

    A path whose `query_ondisk` returns an empty dict (e.g. a failed or unparseable
    `dmls` result) is treated as NOT on disk, so callers keep waiting rather than
    proceeding on a vacuously-true `all([])` result.
    """
    return all([all(d.values()) if d else False
                for d in [query_ondisk(path) for path in paths]])

def mirror_path(path, prefix=f"/vftmp/{getpass.getuser()}"):
    """
    Mirror all files in `path` to location on PP/AN given by `prefix` kwarg.
    Skips any files that have already been mirrored there and waits until all
    copies have completed.
    """
    if type(path)==str:
        path = [path]
    if type(path)==list:
        destination = '/'.join(path[0].split("/")[:-1])
        
        if not(os.path.isdir(f"{prefix}{destination}")):
            os.makedirs(f"{prefix}{destination}", exist_ok=True)
        path_to_copy = [
            p for p in path
            if (not(os.path.isfile(f"{prefix}{p}")) and
                not(os.path.isfile(f"{prefix}{p}.gcp")))
        ]
        time_module.sleep(0.1)
        cmd = f"gcp --debug {' '.join(path)} {prefix}{destination}/"
        print(f"Trying command: {cmd}")
        out = os.system(cmd)
        path = [f"{prefix}{p}".replace("//","/") for p in path]
        while any([not(os.path.isfile(p)) for p in path]):
            time_module.sleep(0.1)
        time_module.sleep(0.1)
    else:
        raise ValueError("path must be str or list of str.")
    return path

def get_ppnames(pp):
    """
    Return the list of folders in the pp directory
    """
    return os.listdir(pp+'/')

def get_local(pp,ppname,out,local1priority="monthly",local2priority="5yr"):
    """
    Retrieve an unknown local file path in pp subdirectory.
    """
    local1 = os.listdir('/'.join([pp,ppname,out]))
    local1 = local1priority if local1priority in local1 else local1[-1]
    local2 = os.listdir('/'.join([pp,ppname,out,local1]))
    local2 = local2priority if local2priority in local2 else local2[-1]
    return '/'.join([local1,local2])

def get_timefrequency(pp,ppname):
    """
    Determine the time frequency of the pp subdirectory based on the local file structure.
    """
    return get_local(pp,ppname,'ts').split('/')[0]

def get_varnames(pp,ppname,verbose=False):
    """
    Return a list of variables in a specific pp subdirectory.
    """
    try:
        valid = True
        local1 = os.listdir('/'.join([pp,ppname,'ts']))[0]
    except:
        valid = False
        if verbose:
            print("No ts directory in "+ppname+". Can't retrieve variables.")

    if valid:
        local = get_local(pp,ppname,'ts')
        files = os.listdir('/'.join([pp,ppname,'ts',local]))

        allvars = []
        for file in files:
            split = file.split('.')
            if 'nc' not in split:
                continue
            else:
                varname = split[-2]
            if varname not in allvars:
                allvars.append(varname)
            else:
                continue
        return allvars

def get_allvars(pp,verbose=False):
    """
    Return a dictionary of all ppnames and their associated variables.
    """
    ppnames = get_ppnames(pp)
    allvars = {}
    for ppname in ppnames:
        varnames = get_varnames(pp,ppname,verbose=verbose)
        if varnames is not None:
            allvars[ppname]=varnames
    return allvars

def find_variable(pp, variable, verbose=False):
    """
    Find the location of a specific variable in the pp folders.
    """
    allvars = get_allvars(pp,verbose=verbose)
    ppnames = []
    found=False
    for ppname in allvars.keys():
        varnames = allvars[ppname]
        if variable in varnames:
            found=True
            if verbose:
                print(variable+' is in '+ppname)
            ppnames.append(ppname)
        else:
            continue
                    
    if found:
        return ppnames
    else:
        print('No '+variable+' in this pp.')

def find_unique_variable(
        pp,
        variable,
        require=[],
        ignore=[],
        unique=True
    ):
    if type(ignore) is str:
        ignore = [ignore]
    if type(require) is str:
        require = [require]
    local_list = [
        e for e in find_variable(pp, variable)
        if (all([r in e for r in require]) and
            not any([s in e for s in ignore]))
    ]
    if len(local_list)==1:
        return local_list[0]
    elif len(local_list)==0:
        raise ValueError("No variables matching these constraints available.")
    elif (len(local_list)>1) and unique:
        raise ValueError("Ambiguous request; more than one ppname"
                         f"containing variable '{variable}' satisfies"
                         f"these constraints: {local_list}.")
    elif (len(local_list)>1) and not(unique):
        return local_list
    
        
def query_is1x1deg(ppname):
    """
    Determine if variables are interpolated onto a 1x1 grid based on the ppname.
    The is predicated on the assumption that the ppname for interpolated data ends
    with '_1x1deg', which is common in current naming conventions.
    """
    if ppname.split('_')[-1]=='1x1deg':
        return True
    else:
        return False
