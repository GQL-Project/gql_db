use iced::window::{Icon};
use iced::{Application, Settings};

use crate::client::gui::welcome_page::window::WelcomePage;

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
    WelcomePage::run(settings)
}
