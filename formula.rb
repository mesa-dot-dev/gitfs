class ${FORMULA_CLASS_NAME} < Formula
  desc "Mount Mesa, GitHub and GitLab repositories as local filesystems via FUSE"
  homepage "${REPO_URL}"
  url "https://github.com/mesa-dot-dev/mesafs/releases/download/v${VERSION}/mesafs-macos-universal.tar.gz"
  version "${VERSION}"
  sha256 "${SHA256}"
  license "MIT"

  depends_on :macos

  def install
    bin.install "mesafs"
  end

  def caveats
    <<~EOS
      mesafs requires macFUSE. Install it from:
        https://macfuse.github.io/

      The Homebrew cask version of macFUSE is outdated.
      We recommend downloading directly from the official site.
    EOS
  end
end
