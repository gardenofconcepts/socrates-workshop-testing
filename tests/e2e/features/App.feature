Feature: EVApp
    I can list my brands

    Scenario: List brands
        Given I have the latest data loaded
        When I search for brand "Hyundai"
        Then I should see "Hyundai" in the brand list
