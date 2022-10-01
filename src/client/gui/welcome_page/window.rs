use iced::widget::Image;
use iced::{
    button, executor, text_input, Alignment, Application, Button, Column, Container, Element,
    Length, Sandbox, Space, Text, TextInput,
};

use crate::client::gui::style::style::DarkMode as Theme;
#[derive(Default)]
pub struct WelcomePage {
    exit: bool,
    theme: Theme,
    login: button::State,
}

#[derive(Debug, Clone)]
pub enum Message {
    ButtonPressed,
}

impl Application for WelcomePage {
    type Executor = executor::Default;

    type Message = Message;

    type Flags = ();

    fn new(_: Self::Flags) -> (Self, iced::Command<Self::Message>) {
        (WelcomePage::default(), iced::Command::none())
    }

    fn title(&self) -> String {
        "GQL - Database Client".to_string()
    }

    fn update(&mut self, message: Self::Message) -> iced::Command<Self::Message> {
        match message {
            Message::ButtonPressed => {
                self.exit = true;
            }
        }

        iced::Command::none()
    }

    fn view(&mut self) -> Element<'_, Self::Message> {
        let image = Image::new("src/client/gui/assets/gql.png")
            .width(Length::Units(128))
            .height(Length::Units(128));
        let content = Column::new()
            .push(Text::new("Welcome to GQL - Database Client").size(50))
            .push(image)
            .push(Space::with_height(Length::Units(50)))
            .push(
                Button::new(&mut self.login, Text::new("Proceed to Login"))
                    .on_press(Message::ButtonPressed)
                    .style(self.theme)
                    .padding(10),
            )
            .spacing(20)
            .padding(20)
            .align_items(Alignment::Center);

        Container::new(content)
            .width(Length::Fill)
            .height(Length::Fill)
            .center_x()
            .center_y()
            .style(Theme::default())
            .into()
    }

    fn should_exit(&self) -> bool {
        self.exit
    }
}
