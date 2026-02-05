class ${CLASS_NAME} < Formula
  desc "Mount Mesa, GitHub and GitLab repositories as local filesystems via FUSE"
  homepage "https://github.com/mesa-dot-dev/git-fs"
  version "${VERSION}"
  url "https://github.com/mesa-dot-dev/git-fs/releases/download/v${VERSION}/git-fs-macos-universal.tar.gz"
  sha256 "${SHA256}"
  license "MIT"

  depends_on :macos

  def install
    bin.install "git-fs"
  end

  def caveats
    <<~EOS
      git-fs requires macFUSE. Install it from:
        https://macfuse.github.io/

      The Homebrew cask version of macFUSE is outdated.
      We recommend downloading directly from the official site.
    EOS
  end

  test do
    assert_match "git-fs", shell_output("#{bin}/git-fs --version", 2)
  end
end
