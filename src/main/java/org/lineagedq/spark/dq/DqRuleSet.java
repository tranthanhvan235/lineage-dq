package org.lineagedq.spark.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Noi dung /dq/rules/{key}/active.json - xem dq-agents/gen_rules_agent.py::_tool_save_accumulated_rules. */
public record DqRuleSet(
        String datasetName,
        String generationRunId,
        String generatedAt,
        String lineageHash,
        List<DqRule> rules,
        JsonNode policy) {

    /** Ban khong co policy - giu cho cac cho dung constructor cu. */
    public DqRuleSet(String datasetName, String generationRunId, String generatedAt,
                     String lineageHash, List<DqRule> rules) {
        this(datasetName, generationRunId, generatedAt, lineageHash, rules,
             JsonNodeFactory.instance.objectNode());
    }

    /**
     * Policy cua mot stage, nhung trong active.json:
     * {"policy": {"src_calls": {"measure": "full", "ask_agent": "always"}}}
     *
     * De o day chu khong de trong decision document cua agent la co y: Java phai doc duoc
     * policy TRUOC khi lam viec Spark, ma decision thi ve SAU khi da do xong. Ngoai ra khoi nay
     * di qua khau verify nen van do agent quyet dinh.
     */
    public JsonNode policyFor(String stage) {
        JsonNode p = policy == null ? null : policy.get(stage);
        return p == null ? JsonNodeFactory.instance.objectNode() : p;
    }

    public static DqRuleSet empty() {
        return new DqRuleSet(null, null, null, null, Collections.emptyList());
    }

    public boolean isEmpty() {
        return rules == null || rules.isEmpty();
    }

    /** Rule cua dung mot checkpoint. Rong = stage do khong co rule nao, khong phai loi. */
    public List<DqRule> forStage(String stage) {
        List<DqRule> out = new ArrayList<>();
        if (rules == null) return out;
        for (DqRule r : rules) {
            if (r.stage().equals(stage)) out.add(r);
        }
        return out;
    }

    /** Cac stage co rule, giu thu tu xuat hien trong active.json. */
    public Set<String> stages() {
        Set<String> out = new LinkedHashSet<>();
        if (rules == null) return out;
        for (DqRule r : rules) out.add(r.stage());
        return out;
    }
}
