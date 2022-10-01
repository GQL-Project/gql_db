use iced::{window::Icon, Application, Settings};

use super::{login::window::Login, welcome_page::window::WelcomePage};

pub fn main() -> iced::Result {
    let mut settings = Settings::default();
    // Read 32bpp image
    let icon = image::load_from_memory(include_bytes!("assets/gql.ico")).map(|image| {
        let rgba = image.to_rgba8().into_raw();
        Icon::from_rgba(rgba, 256, 256)
    });
    if let Ok(Ok(icon)) = icon {
        settings.window.icon = Some(icon);
    } else {
        println!("Failed to load icon {}", icon.unwrap().unwrap_err());
    }
    // There is a better way to do this: Creating a common window with the main "view" and "message"
    // functions, and then delegating the actual view and message to the `current` window.
    WelcomePage::run(settings.clone())?;
    Login::run(settings.clone())?;
    Ok(())
}
