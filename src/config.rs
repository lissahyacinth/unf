#[derive(Clone, Copy)]
pub struct UnfConfigBuilder {
    digits: Option<u32>,
    characters: Option<usize>,
    truncation: Option<usize>,
    version: Option<UnfVersion>,
}

#[derive(Clone, Copy, Debug)]
pub enum UnfVersion {
    Six,
}

impl Default for UnfConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl UnfConfigBuilder {
    pub fn new() -> Self {
        UnfConfigBuilder {
            digits: None,
            characters: None,
            truncation: None,
            version: None,
        }
    }

    pub fn digits(&mut self, x: u32) -> &mut UnfConfigBuilder {
        self.digits = Some(x);
        self
    }

    pub fn characters(&mut self, x: usize) -> &mut UnfConfigBuilder {
        self.characters = Some(x);
        self
    }

    pub fn truncation(&mut self, x: usize) -> &mut UnfConfigBuilder {
        self.truncation = Some(x);
        self
    }

    pub fn version(&mut self, x: UnfVersion) -> &mut UnfConfigBuilder {
        self.version = Some(x);
        self
    }

    pub fn build(&self) -> UnfConfig {
        UnfConfig {
            digits: if let Some(digits) = self.digits {
                digits
            } else {
                7
            },
            truncation: if let Some(truncation) = self.truncation {
                truncation
            } else {
                128
            },
            characters: if let Some(characters) = self.characters {
                characters
            } else {
                128
            },
            version: if let Some(version) = self.version {
                version
            } else {
                UnfVersion::Six
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct UnfConfig {
    pub digits: u32,
    pub truncation: usize,
    pub characters: usize,
    pub version: UnfVersion,
}
