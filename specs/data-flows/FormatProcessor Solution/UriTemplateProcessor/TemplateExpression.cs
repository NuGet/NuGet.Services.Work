﻿namespace UriTemplateProcessor {
    public class TemplateExpression : IUrlPart {
        readonly string _partString;

        public TemplateExpression(string partString) {
            _partString = partString;
        }

        public override string ToString() {
            //TODO
            return string.Format("{{{0}}}", _partString);
        }
    }
}