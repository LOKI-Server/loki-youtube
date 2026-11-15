use super::{Input, Output, Process, available_error};
use crate::Result;
use std::{
    ffi::{OsStr, c_void},
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    },
    path::Path,
    ptr::{null, null_mut},
};
use windows_sys::Win32::{
    Foundation::{HANDLE, HANDLE_FLAG_INHERIT, LocalFree, SetHandleInformation},
    Security::{
        Authorization::ConvertSidToStringSidW,
        FreeSid,
        Isolation::{CreateAppContainerProfile, DeriveAppContainerSidFromAppContainerName},
        PSID, SECURITY_CAPABILITIES,
    },
    System::{JobObjects::*, Threading::*},
};

const PROFILE: &str = "LOKI.YouTube.Worker";

pub(super) fn worker_sid() -> std::io::Result<String> {
    let sid = profile()?;
    let mut text = null_mut();
    check(unsafe { ConvertSidToStringSidW(sid.0, &mut text) })?;
    let length = (0..)
        .find(|index| unsafe { *text.add(*index) == 0 })
        .unwrap_or_default();
    let value = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, length) });
    unsafe {
        LocalFree(text.cast());
    }
    Ok(value)
}

struct Sid(PSID);
impl Drop for Sid {
    fn drop(&mut self) {
        unsafe {
            FreeSid(self.0);
        }
    }
}

struct Attributes(Vec<usize>);
impl Attributes {
    fn new() -> std::io::Result<Self> {
        let mut bytes = 0;
        unsafe {
            InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes);
        }
        let mut data = vec![0usize; bytes.div_ceil(size_of::<usize>())];
        check(unsafe {
            InitializeProcThreadAttributeList(data.as_mut_ptr().cast(), 2, 0, &mut bytes)
        })?;
        Ok(Self(data))
    }

    fn pointer(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.0.as_mut_ptr().cast()
    }

    fn set<T>(&mut self, key: u32, value: &T) -> std::io::Result<()> {
        check(unsafe {
            UpdateProcThreadAttribute(
                self.pointer(),
                0,
                key as usize,
                (value as *const T).cast(),
                size_of::<T>(),
                null_mut(),
                null_mut(),
            )
        })
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe {
            DeleteProcThreadAttributeList(self.pointer());
        }
    }
}

fn check(result: i32) -> std::io::Result<()> {
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn during(error: std::io::Error, stage: &str) -> std::io::Error {
    std::io::Error::new(error.kind(), format!("{stage}: {error}"))
}

fn wide(value: impl AsRef<OsStr>) -> Vec<u16> {
    value
        .as_ref()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn quoted(value: impl AsRef<OsStr>, target: &mut Vec<u16>) {
    let mut backslashes = 0;
    target.push(b'"' as u16);
    for c in value.as_ref().encode_wide() {
        if c == b'\\' as u16 {
            backslashes += 1;
            continue;
        }
        target.extend(std::iter::repeat_n(
            b'\\' as u16,
            if c == b'"' as u16 {
                backslashes * 2 + 1
            } else {
                backslashes
            },
        ));
        backslashes = 0;
        target.push(c);
    }
    target.extend(std::iter::repeat_n(b'\\' as u16, backslashes * 2));
    target.push(b'"' as u16);
}

fn profile() -> std::io::Result<Sid> {
    let name = wide(PROFILE);
    let description = wide("LOKI's no-network YouTube metadata extractor");
    let mut sid = null_mut();
    let status = unsafe {
        CreateAppContainerProfile(
            name.as_ptr(),
            name.as_ptr(),
            description.as_ptr(),
            null(),
            0,
            &mut sid,
        )
    };
    if status < 0 {
        let derive = unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut sid) };
        if derive < 0 {
            return Err(std::io::Error::from_raw_os_error(derive));
        }
    }
    if sid.is_null() {
        return Err(std::io::Error::other("AppContainer SID is missing"));
    }
    Ok(Sid(sid))
}

pub(super) fn start(root: &Path, script: &str) -> Result<(Process, Input, Output)> {
    launch(root, script).map_err(|error| {
        tracing::warn!(%error,"could not start the AppContainer worker");
        available_error()
    })
}

fn launch(root: &Path, script: &str) -> std::io::Result<(Process, Input, Output)> {
    let python = root.join("bin/python.exe");
    let deno = root.join("bin/deno.exe");
    if !python.is_file() || !deno.is_file() {
        return Err(std::io::Error::other("Missing runtime binaries"));
    }
    let sid = profile().map_err(|error| during(error, "AppContainer profile"))?;
    let (child_in, parent_in) = std::io::pipe()?;
    let (parent_out, child_out) = std::io::pipe()?;
    let stderr = std::fs::OpenOptions::new().write(true).open("NUL")?;
    let handles = [
        child_in.as_raw_handle(),
        child_out.as_raw_handle(),
        stderr.as_raw_handle(),
    ];
    for handle in handles {
        check(unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) })?;
    }
    let capabilities = SECURITY_CAPABILITIES {
        AppContainerSid: sid.0,
        Capabilities: null_mut(),
        CapabilityCount: 0,
        Reserved: 0,
    };
    let mut attributes = Attributes::new()?;
    attributes
        .set(PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES, &capabilities)
        .map_err(|error| during(error, "AppContainer process attributes"))?;
    attributes
        .set(PROC_THREAD_ATTRIBUTE_HANDLE_LIST, &handles)
        .map_err(|error| during(error, "worker pipe inheritance"))?;
    let mut info = STARTUPINFOEXW::default();
    info.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    info.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    info.StartupInfo.hStdInput = handles[0];
    info.StartupInfo.hStdOutput = handles[1];
    info.StartupInfo.hStdError = handles[2];
    info.lpAttributeList = attributes.pointer();
    let mut args = Vec::new();
    for value in [
        python.as_os_str(),
        OsStr::new("-I"),
        OsStr::new("-S"),
        OsStr::new("-B"),
        OsStr::new("-c"),
        OsStr::new(script),
        deno.as_os_str(),
    ] {
        if !args.is_empty() {
            args.push(b' ' as u16);
        }
        quoted(value, &mut args);
    }
    args.push(0);
    let system_root = std::env::var_os("SystemRoot")
        .or_else(|| std::env::var_os("WINDIR"))
        .ok_or_else(|| std::io::Error::other("SystemRoot is missing"))?;
    let local = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
        std::io::Error::other("LOCALAPPDATA is required to launch an AppContainer")
    })?;
    let mut environment: Vec<u16> = "LOCALAPPDATA=".encode_utf16().collect();
    environment.extend(wide(local));
    environment.extend(wide("PYTHONUTF8=1"));
    environment.extend("SystemRoot=".encode_utf16());
    environment.extend(wide(system_root));
    environment.push(0);
    let mut process = PROCESS_INFORMATION::default();
    let job = owned(unsafe { CreateJobObjectW(null(), null()) })?;
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        | JOB_OBJECT_LIMIT_ACTIVE_PROCESS
        | JOB_OBJECT_LIMIT_JOB_MEMORY
        | JOB_OBJECT_LIMIT_PROCESS_TIME;
    limits.BasicLimitInformation.ActiveProcessLimit = 8;
    limits.BasicLimitInformation.PerProcessUserTimeLimit = 60 * 10_000_000;
    limits.JobMemoryLimit = 768 * 1024 * 1024;
    check(unsafe {
        SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            size_of_val(&limits) as u32,
        )
    })
    .map_err(|error| during(error, "worker job limits"))?;
    check(unsafe {
        CreateProcessW(
            wide(&python).as_ptr(),
            args.as_mut_ptr(),
            null(),
            null(),
            1,
            CREATE_SUSPENDED
                | CREATE_NO_WINDOW
                | CREATE_UNICODE_ENVIRONMENT
                | EXTENDED_STARTUPINFO_PRESENT,
            environment.as_ptr().cast::<c_void>(),
            wide(root).as_ptr(),
            &info.StartupInfo,
            &mut process,
        )
    })
    .map_err(|error| during(error, "starting sandboxed Python"))?;
    let process_handle = owned(process.hProcess)?;
    let thread = owned(process.hThread)?;
    if let Err(error) = check(unsafe {
        AssignProcessToJobObject(job.as_raw_handle(), process_handle.as_raw_handle())
    }) {
        unsafe {
            TerminateProcess(process_handle.as_raw_handle(), 1);
        }
        return Err(during(error, "assigning worker to Job Object"));
    }
    if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
        return Err(std::io::Error::last_os_error());
    }
    drop((child_in, child_out, stderr));
    let stdin = tokio::fs::File::from_std(std::fs::File::from(OwnedHandle::from(parent_in)));
    let stdout = tokio::fs::File::from_std(std::fs::File::from(OwnedHandle::from(parent_out)));
    Ok((Process { job }, Box::new(stdin), Box::new(stdout)))
}

fn owned(handle: HANDLE) -> std::io::Result<OwnedHandle> {
    if handle.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}
