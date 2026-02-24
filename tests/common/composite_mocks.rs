#![allow(missing_docs, clippy::unwrap_used)]

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use git_fs::fs::INode;
use git_fs::fs::composite::{ChildDescriptor, CompositeRoot};

use super::async_fs_mocks::MockFsDataProvider;

/// A mock `CompositeRoot` that resolves children from a fixed map.
pub struct MockRoot {
    pub children: Arc<HashMap<OsString, (MockFsDataProvider, INode)>>,
}

impl MockRoot {
    pub fn new(children: HashMap<OsString, (MockFsDataProvider, INode)>) -> Self {
        Self {
            children: Arc::new(children),
        }
    }
}

impl CompositeRoot for MockRoot {
    type ChildDP = MockFsDataProvider;

    async fn resolve_child(
        &self,
        name: &OsStr,
    ) -> Result<Option<ChildDescriptor<MockFsDataProvider>>, std::io::Error> {
        Ok(self
            .children
            .get(name)
            .map(|(provider, root_ino)| ChildDescriptor {
                name: name.to_os_string(),
                provider: provider.clone(),
                root_ino: *root_ino,
            }))
    }

    async fn list_children(
        &self,
    ) -> Result<Vec<ChildDescriptor<MockFsDataProvider>>, std::io::Error> {
        Ok(self
            .children
            .iter()
            .map(|(name, (provider, root_ino))| ChildDescriptor {
                name: name.clone(),
                provider: provider.clone(),
                root_ino: *root_ino,
            })
            .collect())
    }
}
