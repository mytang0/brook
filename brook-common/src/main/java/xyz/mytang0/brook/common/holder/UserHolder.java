package xyz.mytang0.brook.common.holder;

import xyz.mytang0.brook.common.metadata.model.User;

public abstract class UserHolder {

    private static final ThreadLocal<User> currentUser = ThreadLocal.withInitial(() -> null);

    public static void setCurrentUser(User user) {
        currentUser.set(user);
    }

    public static void clearCurrentUser() {
        currentUser.remove();
    }

    public static User getCurrentUser() {
        return currentUser.get();
    }
}
