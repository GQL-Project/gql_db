use iced::widget::Image;
use iced::{
    button, executor, text_input, Alignment, Application, Button, Column, Container, Element,
    Length, Sandbox, Space, Text, TextInput,
};
#[derive(Default)]
pub struct WelcomePage {
    email: String,
    password: String,

    input_email: text_input::State,
    input_password: text_input::State,
    login: button::State,
}

#[derive(Debug, Clone)]
pub enum Message {
    EmailChanged(String),
    PasswordChanged(String),
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
            Message::EmailChanged(email) => self.email = email,
            Message::PasswordChanged(password) => self.password = password,
            Message::ButtonPressed => {
                println!("{} {}", self.email, self.password);
            }
        }

        iced::Command::none()
    }

    fn view(&mut self) -> Element<'_, Self::Message> {
        let image = Image::new("src/client/gui/welcome_page/assets/gql.png")
            .width(Length::Units(256))
            .height(Length::Units(256));
        let content = Column::new()
            .push(Text::new("Welcome to GQL - Database Client").size(50))
            .push(image)
            .push(Space::with_height(Length::Units(50)))
            .push(
                TextInput::new(
                    &mut self.input_email,
                    "Email",
                    &self.email,
                    Message::EmailChanged,
                )
                .padding(10)
                .size(20),
            )
            .push(
                TextInput::new(
                    &mut self.input_password,
                    "Password",
                    &self.password,
                    Message::PasswordChanged,
                )
                .on_submit(Message::ButtonPressed)
                .padding(10)
                .password()
                .size(20),
            )
            .push(Button::new(&mut self.login, Text::new("Login")).on_press(Message::ButtonPressed))
            .spacing(20)
            .padding(20)
            .align_items(Alignment::Center);

        Container::new(content)
            .width(Length::Fill)
            .height(Length::Fill)
            .center_x()
            .center_y()
            .into()
    }
}
