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
            return Err("Invalid GitHub repository format. Expected 'org/repo'.".to_string());
        }
        Ok(GhRepoInfo {
            org: parts[0].to_string(),
            repo: parts[1].to_string(),
        })
    }
}
