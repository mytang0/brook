package xyz.mytang0.brook.spi.annotation.selected;

import xyz.mytang0.brook.common.extension.Selected;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Documented
@Target(TYPE)
@Inherited
@Selected
@Retention(RetentionPolicy.RUNTIME)
public @interface Level1SelectedSPI {
}
