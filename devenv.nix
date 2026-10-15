{
  pkgs,
  ...
}:

{
  packages = with pkgs; [ cargo-watch ];

  languages.rust = {
    enable = true;
    lsp.enable = true;
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
    ];
  };

  git-hooks.hooks = {
    shellcheck.enable = true;
    prettier.enable = true;
    rustfmt.enable = true;
    nixfmt.enable = true;
    clippy.enable = true;
  };

  # See full reference at https://devenv.sh/reference/options/
}
