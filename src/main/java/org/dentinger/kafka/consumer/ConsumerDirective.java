package org.dentinger.kafka.consumer;

import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ConsumerDirective {

  public enum Directive {
    PROCESS("process"), DRAIN("drain"), PAUSE("pause");

    private String value;

    Directive(String value) {
      this.value = value;
    }

    static Directive getDirectiveForValue(String value) {

      return
        Arrays.stream(values())
          .filter( it -> it.value.equals(value) )
          .findFirst().orElse(null);
    }
  }

  private Directive currentDirective = Directive.PROCESS;

  public boolean isDrain() {

    return currentDirective == Directive.DRAIN;
  }

  public void setCurrentDirective(Directive directive) {
    this.currentDirective = directive;
  }
}
