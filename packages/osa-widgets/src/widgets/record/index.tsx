import type { RecordDetailData } from "../../lib/types";
import { mountWidget } from "../mount";
import { RecordDetail } from "./RecordDetail";

mountWidget<RecordDetailData>((data, host) => <RecordDetail data={data} host={host} />);
