use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use crate::errors::BoxError;
use crate::notify::watch::Interest;
use crate::runtime::Runtime;
use crate::wake::Wake;

pub use types::Pending;
pub(crate) use types::{shut_door, Inner, Waiter};

pub struct Drive {
    runtime: Rc<RefCell<Runtime>>,
    inner: Rc<RefCell<Inner>>,
    wake: RefCell<Option<Rc<Wake>>>,
    task: RefCell<Option<tokio::task::JoinHandle<()>>>,
    closed: Rc<Cell<bool>>,
    pumping: Rc<Cell<bool>>,
    releasing: Rc<Cell<bool>>,
    fault: Rc<RefCell<Option<BoxError>>>,
    slf: RefCell<Weak<Drive>>,
    space_kicks: RefCell<HashMap<usize, Rc<Interest>>>,
    /// 泵转过几圈。py 的测试盖住 `Runtime.advance` 数圈（`test_drive.py` 的 `_Laps`）；
    laps: Cell<usize>,
}

mod base;
mod lifecycle;
mod open;
mod release;
mod run;
mod types;
