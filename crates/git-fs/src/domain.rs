use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct GhRepoInfo {
    pub org: String,
    pub repo: String,
}

impl FromStr for GhRepoInfo {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err("Invalid GitHub repository format. Expected 'org/repo'.".to_owned());
        }
        Ok(Self {
            org: parts[0].to_owned(),
            repo: parts[1].to_owned(),
        })
    }
}
