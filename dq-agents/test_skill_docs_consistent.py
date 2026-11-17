"""Sau tai lieu skill duoc nap chung MOT system prompt - chung khong duoc mau thuan nhau.

Da xay ra that: dq-rule-generator/SKILL.md muc 3b bao "viet rule duy nhat bang kind=dataset"
va dua vi du `count(DISTINCT call_id) <> count(call_id)`, trong khi dq-sql-generator/
OUTPUT_FORMAT.md xep DUNG bieu thuc do vao bang "KHONG viet duoc - Spark se bao loi va hong ca
lan chay". Agent nghe lenh cam. Ket qua: khong mot rule uniqueness nao duoc sinh ra, du liieu
trung khoa lot xuong bang dich.

Lenh cam do dung o thoi rule chay bang observe(); checkpoint gio dung df.agg() nen het dung,
nhung tai lieu khong ai cap nhat. Luoi nay chan viec quay lai.

    python3 test_skill_docs_consistent.py
"""
import ast
import os
import sys
from pathlib import Path

import skill_prompt_loader

SKILLS = ("dq-rule-generator", "dq-sql-generator", "dq-rule-verifier")
#: Prompt khong chi nam trong .md - cac hang so nay duoc noi vao system prompt va duoc noi SAU
#: skill docs, nen khi mau thuan thi chung thang.
PROMPT_SOURCES = ("gen_rules_agent.py",)
#: Thu muc chua chinh file test - cac module agent nam canh no.
HERE = Path(__file__).resolve().parent
#: Duoi nguong nay coi la ten truong/dinh danh chu khong phai loi dan cho agent.
PROSE_MIN_CHARS = 40
DOCS = ("SKILL.md", "METHODOLOGY.md", "OUTPUT_FORMAT.md")

#: Cac phat bieu da tung dung nhung gio SAI. Xuat hien lai la tai lieu bi keo ve ban cu.
STALE_CLAIMS = (
    "tính duy nhất của khoá grain thì **không viết được**",
    "đừng cố nhét vào sql",
    "đừng cố nhét vào failed_sql_expr",
    "không aggregate, không window, không subquery",
    "luôn bẫy null",
    "luôn có nhánh bắt giá trị null",
)

#: Tai lieu nao dat rang buoc len failed_sql_expr thi PHAI noi ro rang buoc do theo kind nao.
#: Thieu chu `kind` nghia la rang buoc duoc hieu la ap cho moi rule - dung cai bay cu.
CONSTRAINT_MARKERS = ("mức dòng", "muc dong", "aggregate")



def check_rule_fields_reach_the_store():
    """Moi truong schema add_rules khai PHAI di duoc toi kho. Tra ve so loi.

    Kieu hong nay da xay ra hai lan theo dung mot cach: schema khai them mot truong, ham
    copy trong _tool_add_rules khong sua theo, truong bi vut am tham. Lan gan nhat la
    `observed_unit` - cong tham dinh BAT BUOC no voi rule kind=dataset, agent gui dung,
    add_rules vut di, tham dinh tu choi, agent gui lai, vut tiep. Mot luot 116 rule chet
    vi vong do, va khong log nao noi ra duoc vi khong ai bao gi ca.
    """
    try:
        import gen_rules_agent
        import verify_rules_agent
    except Exception as e:                      # thieu phu thuoc LLM -> bo qua, khong bao sai
        print(f"BO QUA hop dong truong rule: khong nap duoc module ({type(e).__name__})")
        return 0

    errors = 0
    declared = set(gen_rules_agent.RULE_REQUIRED_FIELDS) | set(
        gen_rules_agent.RULE_OPTIONAL_FIELDS)

    for tool in gen_rules_agent.TOOLS_SCHEMA:
        fn = tool.get("function") or {}
        if fn.get("name") != "add_rule":
            continue
        single = set(fn["parameters"]["properties"])
        if single != declared:
            errors += 1
            print(f"LOI  add_rule va add_rules khai khac nhau:"
                  f" chi add_rule co {sorted(single - declared)},"
                  f" chi add_rules co {sorted(declared - single)}")

    # Truong ma cong tham dinh BAT BUOC nhung schema khong khai = agent khong the gui.
    for field in ("kind", "observed_expr", "observed_unit"):
        if field not in declared:
            errors += 1
            print(f"LOI  verify doi '{field}' nhung schema add_rules khong khai -"
                  f" agent khong co cach nao gui no")

    if set(verify_rules_agent.OBSERVED_UNITS) != set(gen_rules_agent.OBSERVED_UNITS):
        errors += 1
        print(f"LOI  OBSERVED_UNITS lech giua gen va verify:"
              f" {verify_rules_agent.OBSERVED_UNITS} vs {gen_rules_agent.OBSERVED_UNITS}")

    # Hai ban mentions_null phai cho cung ket qua. Doi chieu bang HANH VI chu khong bang ma
    # nguon: hai ham co the viet khac nhau ma van dung, nhung lech mot truong hop la gen cho
    # qua mot rule ma verify se danh rot (hoac nguoc lai) - va agent khong the sua noi vi loi
    # bao o mot khau con luat nam o khau kia.
    null_cases = [
        "col IS NULL",                          # NULL la vi pham
        "col IS NOT NULL AND col < 0",          # NULL hop le, chi kiem mien gia tri
        "is_congested AND col IS NULL",         # NULL chi vi pham khi dieu kien dung
        "col < 0",                              # khong noi gi ve NULL -> phai bi chan
        "COL IS NULL OR col > 100",
        "trim(col) = ''",
    ]
    for expr in null_cases:
        g = gen_rules_agent.mentions_null(expr)
        v = verify_rules_agent.mentions_null(expr)
        if g != v:
            errors += 1
            print(f"LOI  mentions_null lech giua gen ({g}) va verify ({v}) voi: {expr!r}")

    if errors == 0:
        print(f"     hop dong truong rule: {len(declared)} truong di tron tu schema toi kho.")
    return errors


def main():
    root = skill_prompt_loader.resolve_skills_dir()
    failed = check_rule_fields_reach_the_store() > 0
    checked = 0

    for skill in SKILLS:
        for doc in DOCS:
            path = root / skill / doc
            if not path.exists():
                continue
            checked += 1
            text = path.read_text(encoding="utf-8")
            lower = text.lower()
            name = f"{skill}/{doc}"

            # Xet theo DONG va co nhin dong lien ke: mot phat bieu co `kind` dinh pham vi
            # ngay canh la phat bieu da duoc sua dung, khong phai tai pham.
            lines = lower.splitlines()
            for index, line in enumerate(lines):
                near = " ".join(lines[max(index - 1, 0):index + 2])
                for claim in STALE_CLAIMS:
                    if claim in line and "kind" not in near:
                        failed = True
                        print(f"LOI  {name}:{index + 1}: phat bieu da lac hau -> \"{claim}\"")

            # Noi ve gioi han bieu thuc ma khong nhac kind -> hieu nham la cam tat ca.
            if any(m in lower for m in CONSTRAINT_MARKERS) and "kind" not in lower:
                failed = True
                print(f"LOI  {name}: dat rang buoc len bieu thuc nhung khong phan biet `kind`")

    # Prompt trong ma Python cung la prompt. Lan dau luoi nay chi soi .md, nen mau thuan chi
    # chuyen cho: OUTPUT_INVARIANTS trong gen_rules_agent.py khai schema rule 5 truong khong
    # co `kind`, va no duoc noi SAU CUNG nen agent nghe no nhat - ket qua van 0 rule muc tap.
    for name in PROMPT_SOURCES:
        path = HERE / name
        if not path.exists():
            continue
        checked += 1
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        # CHI soi ben trong chuoi: dong 898 la ma kiem tra truong bat buoc, o do vang `kind`
        # la DUNG vi no la truong tuy chon. Luoi nay noi ve PROMPT, khong noi ve ma.
        # Chi tinh chuoi VAN XUOI: `"failed_sql_expr"` o dong 898 cung la chuoi, nhung no la
        # TEN TRUONG trong ma kiem tra, khong phai loi dan cho agent.
        string_lines = set()
        for node in ast.walk(ast.parse(source, filename=str(path))):
            if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                    and len(node.value) >= PROSE_MIN_CHARS and " " in node.value.strip()):
                string_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
        for index, line in enumerate(lines):
            if index + 1 not in string_lines:
                continue
            lower = line.lower()
            near = " ".join(lines[max(index - 3, 0):index + 4]).lower()
            if "null" in lower and ("bẫy" in lower or "bay null" in lower) and "kind" not in near:
                failed = True
                print(f"LOI  {name}:{index + 1}: doi hoi bay NULL ma khong phan biet `kind`")
            # Cho nao liet ke cac truong bat buoc cua rule thi phai ke ca kind.
            if "failed_sql_expr" in lower and "importance_weight" in lower and "kind" not in near:
                failed = True
                print(f"LOI  {name}:{index + 1}: liet ke truong rule nhung bo sot `kind`")

    # Hop dong rule muc tap phai duoc neu day du o noi dinh nghia schema.
    for skill in SKILLS:
        path = root / skill / "OUTPUT_FORMAT.md"
        if not path.exists():
            continue
        lower = path.read_text(encoding="utf-8").lower()
        for field in ("observed_expr", "expected"):
            if field not in lower:
                failed = True
                print(f"LOI  {skill}/OUTPUT_FORMAT.md: thieu hop dong `{field}` cua kind=dataset")

    print()
    if failed:
        print("HONG: tai lieu skill mau thuan - agent se nghe theo cau cam.")
        return 1
    print(f"OK: {checked} tai lieu skill thong nhat ve `kind`, khong con phat bieu lac hau.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

