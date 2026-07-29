import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import { TagInput } from "./TagInput";

function Harness({ initial = [] }: { initial?: string[] }) {
  const [values, setValues] = useState<string[]>(initial);
  return (
    <TagInput
      values={values}
      onChange={setValues}
      label="Administrator ORCID iDs"
      placeholder="Add an iD…"
    />
  );
}

describe("TagInput", () => {
  it("adds a tag on Enter and clears the input", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const input = screen.getByRole("textbox", {
      name: "Administrator ORCID iDs",
    });
    await user.type(input, "0000-0002-1825-0097{Enter}");
    expect(screen.getByText("0000-0002-1825-0097")).toBeInTheDocument();
    expect(input).toHaveValue("");
  });

  it("does not add duplicates or empty values", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["0000-0002-1825-0097"]} />);
    const input = screen.getByRole("textbox");
    await user.type(input, "0000-0002-1825-0097{Enter}");
    await user.type(input, "   {Enter}");
    expect(screen.getAllByText("0000-0002-1825-0097")).toHaveLength(1);
  });

  it("removes a tag via its remove button", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["0000-0002-1825-0097"]} />);
    await user.click(
      screen.getByRole("button", { name: /remove 0000-0002-1825-0097/i }),
    );
    expect(screen.queryByText("0000-0002-1825-0097")).not.toBeInTheDocument();
  });

  it("removes the last tag with Backspace on an empty input", async () => {
    const user = userEvent.setup();
    render(<Harness initial={["0000-0001-5109-3700"]} />);
    await user.click(screen.getByRole("textbox"));
    await user.keyboard("{Backspace}");
    expect(screen.queryByText("0000-0001-5109-3700")).not.toBeInTheDocument();
  });
});
