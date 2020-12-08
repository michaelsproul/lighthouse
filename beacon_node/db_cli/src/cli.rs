use clap::{App, Arg};

pub fn cli_app<'a, 'b>() -> App<'a, 'b> {
    App::new("database")
        .visible_aliases(&["db"])
        .author("Sigma Prime <contact@sigmaprime.io>")
        .setting(clap::AppSettings::ColoredHelp)
        .about("Utility for manual database surgery. To be used sparingly and carefully.")
        .subcommand(
            App::new("delete-from-fork-choice")
                .arg(
                    Arg::with_name("block-root")
                        .value_name("BLOCK_ROOT")
                        .required(true)
                        .takes_value(true),
                )
                .help(
                    "Delete a block from fork choice. If your database is already corrupted \
                     this *might* bring it back, but could also make it worse.",
                ),
        )
}
