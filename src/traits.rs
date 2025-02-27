use crate::SortState;

use egui::{
    Context,
    FontFamily::Proportional,
    FontId, Response,
    TextStyle::{Body, Button, Heading, Monospace, Small},
    Ui, WidgetText,
};

/// A trait for applying custom styling to the egui context.
pub trait MyStyle {
    /// Sets the initial style for the egui context.
    fn set_style_init(&self);
}

impl MyStyle for Context {
    /// Specifies the look and feel of egui.
    ///
    /// <https://docs.rs/egui/latest/egui/style/struct.Style.html>
    fn set_style_init(&self) {
        // Get current context style
        let mut style = (*self.style()).clone();

        // Redefine text_styles
        style.text_styles = [
            (Small, FontId::new(12.0, Proportional)),
            (Body, FontId::new(16.0, Proportional)),
            (Monospace, FontId::new(14.0, Proportional)),
            (Button, FontId::new(14.0, Proportional)),
            (Heading, FontId::new(14.0, Proportional)),
        ]
        .into();

        style.spacing.item_spacing.x = 8.0;
        style.spacing.item_spacing.y = 6.0;

        // Mutate global style with above changes
        self.set_style(style);
    }
}

// Trait for popover windows.
pub trait Popover {
    fn show(&mut self, ctx: &Context) -> bool;
}

// Settings popover struct (currently disabled).
pub struct Settings {}

impl Popover for Settings {
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        // Create a window named "Settings".
        egui::Window::new("Settings")
            .collapsible(false) // Make the window non-collapsible.
            .open(&mut open) // Control the window's open state.
            .show(ctx, |ui| {
                ctx.style_ui(ui, egui::Theme::Dark); // Apply dark theme.
                ui.disable(); // Disable user interaction.
            });

        open // Return whether the window is open.
    }
}

// Error popover struct.
pub struct Error {
    pub message: String,
}

impl Popover for Error {
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        // Create a window named "Error".
        egui::Window::new("Error")
            .collapsible(false) // Make the window non-collapsible.
            .open(&mut open) // Control the window's open state.
            .show(ctx, |ui| {
                ui.label(format!("Error: {}", self.message)); // Display the error message.
                ui.disable(); // Disable user interaction.
            });

        open // Return whether the window is open.
    }
}

// Trait for selection depth, used for sort state.
pub trait SelectionDepth<Icon> {
    fn inc(&self) -> Self; // Increment the selection depth/state.

    fn reset(&self) -> Self; // Reset the selection depth/state.

    fn format(&self) -> Icon
    where
        Icon: Into<WidgetText>; // Format the selection depth/state.
}

// Trait implementation to increment the sort state.
impl SelectionDepth<String> for SortState {
    fn inc(&self) -> Self {
        match self {
            SortState::NotSorted(col) => SortState::Descending(col.to_owned()), // Not Sorted -> Descending.
            SortState::Ascending(col) => SortState::Descending(col.to_owned()), // Ascending -> Descending.
            SortState::Descending(col) => SortState::Ascending(col.to_owned()), // Descending -> Ascending.
        }
    }

    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        match self {
            SortState::NotSorted(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
            SortState::Ascending(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
            SortState::Descending(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
        }
    }

    fn format(&self) -> String {
        match self {
            SortState::Descending(col) => format!("\u{23f7} {}", col), // Format for Descending.
            SortState::Ascending(col) => format!("\u{23f6} {}", col),  // Format for Ascending.
            SortState::NotSorted(col) => format!("\u{2195} {}", col),  // Format for Not Sorted.
        }
    }
}

// Trait for extra UI interactions.
pub trait ExtraInteractions {
    // Creates a sort button.
    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response;
}

// Implementation of ExtraInteractions for Ui.
impl ExtraInteractions for Ui {
    // Implementation of the sort button.
    fn sort_button<Value: PartialEq + SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Value>,
        selected_value: Value,
    ) -> Response {
        let selected = match current_value {
            Some(value) => *value == selected_value, // Check if the value is selected.
            None => false,
        };
        let mut response = self.selectable_label(selected, selected_value.format()); // Create a selectable label as a button.
        if response.clicked() {
            // If the button is clicked.
            if selected {
                *current_value = Some(selected_value.inc()); // Increment the value.
            } else {
                if let Some(value) = current_value {
                    value.reset(); // Reset the value.
                }
                *current_value = Some(selected_value.inc()); // Increment the value.
            };
            response.mark_changed(); // Mark the response as changed.
        }
        response // Return the response.
    }
}
