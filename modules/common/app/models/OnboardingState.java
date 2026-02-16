package models;

public enum OnboardingState {
    // states for main status of researcher: 0, 1, 2
    NEVER,
    ACTIVE,
    NEXTDAY,

    // states for scenes: 3, 4, 5
    INITIAL,
    ONGOING,
    FINISH;

}