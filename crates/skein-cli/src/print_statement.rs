use std::fmt::Display;
use std::io::{self, Write};

const CYAN: &str = "\x1b[36m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const BOLD_CYAN: &str = "\x1b[1;36m";
const BOLD_GREEN: &str = "\x1b[1;32m";
const RESET: &str = "\x1b[0m";

pub fn print_banner<ID: Display>(id: &ID) {
    println!("{CYAN}");
    println!("                                                                     ");
    println!("                                                                     ");
    println!("               WELCOME TO                                            ");
    println!("                                                                     ");
    println!("               ███████╗██╗  ██╗███████╗██╗███╗   ██╗                 ");
    println!("               ██╔════╝██║ ██╔╝██╔════╝██║████╗  ██║                 ");
    println!("               ███████╗█████╔╝ █████╗  ██║██╔██╗ ██║                 ");
    println!("               ╚════██║██╔═██╗ ██╔══╝  ██║██║╚██╗██║                 ");
    println!("               ███████║██║  ██╗███████╗██║██║ ╚████║                 ");
    println!("               ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝╚═╝  ╚═══╝                 ");
    println!("                                                                     ");
    println!("                                                                     ");
    println!("{RESET}");
    println!();

    println!("Your ID:   {BOLD_GREEN}{id}{RESET}");
    println!();

    println!(
        "Commands:  {YELLOW}/connect <id>{RESET}   \
         {YELLOW}/send <message>{RESET}   \
         {YELLOW}/sendfile <path>{RESET}"
    );
    println!();
}

pub fn print_help() {
    println!("{CYAN}");
    println!("                                  ");
    println!("                                  ");
    println!("  ██╗  ██╗███████╗██╗     ██████╗ ");
    println!("  ██║  ██║██╔════╝██║     ██╔══██╗");
    println!("  ███████║█████╗  ██║     ██████╔╝");
    println!("  ██╔══██║██╔══╝  ██║     ██╔═══╝ ");
    println!("  ██║  ██║███████╗███████╗██║     ");
    println!("  ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝     ");
    println!("                                  ");
    println!("                                  ");
    println!("{RESET}");

    println!("{CYAN}  COMMAND REFERENCE{RESET}");
    println!();

    println!("  {YELLOW}connect <id>{RESET}             Connect to a peer");
    println!("  {YELLOW}send <msg> / s <msg>{RESET}     Broadcast a text message");
    println!("  {YELLOW}sendfile <path> / sf{RESET}     Send a file");
    println!("  {YELLOW}id / whoami / iam{RESET}        Display your endpoint ID");
    println!("  {YELLOW}help / h / ?{RESET}             Show this help");
    println!("  {YELLOW}quit / exit / q / bye{RESET}    Leave the chat");
    println!();

    println!("  {GREEN}<message>{RESET}                Send message directly");
    println!();
}

pub fn print_prompt() {
    print!("{BOLD_CYAN}skein>{RESET} ");
    io::stdout().flush().ok();
}

pub fn print_id<ID: Display>(id: &ID) {
    println!("Your ID:   {BOLD_GREEN}{id}{RESET}");
}
