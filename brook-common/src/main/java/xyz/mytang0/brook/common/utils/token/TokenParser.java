package xyz.mytang0.brook.common.utils.token;

import java.util.ArrayList;
import java.util.List;

public final class TokenParser {

    private final String openToken;
    private final String closeToken;
    private final TokenHandler handler;

    public TokenParser(String openToken, String closeToken, TokenHandler handler) {
        this.openToken = openToken;
        this.closeToken = closeToken;
        this.handler = handler;
    }

    public String parse(String text) {
        if (text != null && !text.isEmpty()) {
            char[] src = text.toCharArray();
            int offset = 0;
            int start = text.indexOf(this.openToken, offset);
            if (start == -1) {
                return text;
            } else {
                StringBuilder builder = new StringBuilder();

                for (StringBuilder expression = null; start > -1; start = text.indexOf(this.openToken, offset)) {
                    if (start > 0 && src[start - 1] == '\\') {
                        builder.append(src, offset, start - offset - 1).append(this.openToken);
                        offset = start + this.openToken.length();
                    } else {
                        if (expression == null) {
                            expression = new StringBuilder();
                        } else {
                            expression.setLength(0);
                        }

                        builder.append(src, offset, start - offset);
                        offset = start + this.openToken.length();

                        int end;
                        for (end = text.indexOf(this.closeToken, offset); end > -1; end = text.indexOf(this.closeToken, offset)) {
                            if (end <= offset || src[end - 1] != '\\') {
                                expression.append(src, offset, end - offset);
                                break;
                            }

                            expression.append(src, offset, end - offset - 1).append(this.closeToken);
                            offset = end + this.closeToken.length();
                        }

                        if (end == -1) {
                            builder.append(src, start, src.length - start);
                            offset = src.length;
                        } else {
                            builder.append(this.handler.handleToken(expression.toString()));
                            offset = end + this.closeToken.length();
                        }
                    }
                }

                if (offset < src.length) {
                    builder.append(src, offset, src.length - offset);
                }

                return builder.toString();
            }
        } else {
            return "";
        }
    }

    public List<String> getVariables(String text) {
        List<String> variables = new ArrayList<>();
        if (text != null && !text.isEmpty()) {
            char[] src = text.toCharArray();
            int offset = 0;
            int start = text.indexOf(this.openToken, offset);
            if (start == -1) {
                return variables;
            } else {
                for (StringBuilder expression = null; start > -1; start = text.indexOf(this.openToken, offset)) {
                    if (start > 0 && src[start - 1] == '\\') {
                        offset = start + this.openToken.length();
                    } else {
                        if (expression == null) {
                            expression = new StringBuilder();
                        } else {
                            expression.setLength(0);
                        }

                        offset = start + this.openToken.length();

                        int end;
                        for (end = text.indexOf(this.closeToken, offset); end > -1; end = text.indexOf(this.closeToken, offset)) {
                            if (end <= offset || src[end - 1] != '\\') {
                                expression.append(src, offset, end - offset);
                                break;
                            }

                            expression.append(src, offset, end - offset - 1).append(this.closeToken);
                            offset = end + this.closeToken.length();
                        }

                        if (end == -1) {
                            offset = src.length;
                        } else {
                            variables.add(expression.toString());
                            offset = end + this.closeToken.length();
                        }
                    }
                }

                return variables;
            }
        } else {
            return null;
        }
    }
}
